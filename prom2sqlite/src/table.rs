// SQL-like abstractions used as an intermediate represntation .
// Copyright (C) 2024, Tony Rippy
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use parse::{parse, MetricFamily, Sample, SampleType};
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ColumnType {
    String,
    Integer,
    Float,
}

type TableSchema = Vec<(String, ColumnType)>;

pub struct TableWriter {
    instance: Option<String>,
    job: Option<String>,
    connection: sqlite::Connection,
    schemas: HashMap<String, TableSchema>,
}

impl TableWriter {
    pub fn open(database: &str) -> sqlite::Result<TableWriter> {
        Ok(TableWriter {
            instance: None,
            job: None,
            connection: sqlite::open(database)?,
            schemas: HashMap::new(),
        })
    }

    pub fn set_instance(&mut self, instance: &str) {
        self.instance = Some(instance.to_owned());
    }

    pub fn set_job(&mut self, job: &str) {
        self.job = Some(job.to_owned());
    }

    fn schema_for(&self, family: &MetricFamily) -> TableSchema {
        let mut schema = Vec::new();
        if self.instance.is_some() {
            schema.push(("instance".to_string(), ColumnType::String));
        }
        if self.job.is_some() {
            schema.push(("job".to_string(), ColumnType::String));
        }
        // Find labels that should be columns in the table.
        let mut labels = BTreeSet::<&str>::new();
        for sample in &family.samples {
            labels.extend(sample.labels.iter().map(|(name, _)| name.to_owned()));
        }
        // Remove any columns that should be handled specially.
        match family.r#type {
            SampleType::Summary => {
              labels.remove("quantile");
            }
            SampleType::Histogram => {
              labels.remove("le");
            }
            _ => {}
        }

        for label in labels {
            schema.push((label.to_string(), ColumnType::String));
        }
        // Add a column for the sampled value.
        match family.r#type {
          SampleType::Counter => {
              schema.push(("value".to_string(), ColumnType::Integer));
          }
          SampleType::Gauge => {
              schema.push(("value".to_string(), ColumnType::Float));
          }
          SampleType::Summary => {
              let mut quantiles = BTreeSet::<&str>::new();
              for sample in &family.samples {
                for &(_, quantile) in sample.labels.iter().filter(|(name, _)| *name == "quantile") {
                  quantiles.insert(quantile);
                }
              }
              for quantile in quantiles {
                schema.push((format!("\"quantile={}\"", quantile), ColumnType::Float));
              }
              schema.push(("count".to_string(), ColumnType::Integer));
              schema.push(("sum".to_string(), ColumnType::Float));
          }
          SampleType::Histogram => {
              let mut buckets = BTreeSet::<&str>::new();
              for sample in &family.samples {
                for &(_, bucket) in sample.labels.iter().filter(|(name, _)| *name == "le") {
                  buckets.insert(bucket);
                }
              }
              for bucket in buckets {
                schema.push((format!("\"value<={}\"", bucket), ColumnType::Integer));
              }
              schema.push(("count".to_string(), ColumnType::Integer));
              schema.push(("sum".to_string(), ColumnType::Float));
          }
          SampleType::Untyped => {
              schema.push(("value".to_string(), ColumnType::String));
          }
      }
      schema
    }

    fn create_table(&mut self, table: &str, schema: &TableSchema) -> String {
        let mut columns = Vec::new();
        for (name, column_type) in schema.iter() {
            let column_type = match column_type {
                ColumnType::String => "TEXT",
                ColumnType::Integer => "INTEGER",
                ColumnType::Float => "REAL",
            };
            columns.push(format!("{} {}", name, column_type));
        }
        let columns = columns.join(", ");
        format!("CREATE TABLE IF NOT EXISTS \"{}\" (timestamp DATETIME NOT NULL, {})", table, columns)
    }

    fn insert_vanilla_samples(
        &self,
        timestamp_millis: u64,
        sample: &Sample,
        table: &str,
        schema: &TableSchema,
    ) -> String {
        let mut columns = Vec::new();
        let mut values = Vec::new();
        columns.push("timestamp".to_string());
        values.push(format!("{:?}", chrono::DateTime::from_timestamp_millis(timestamp_millis as i64).unwrap().to_rfc3339()));
        if self.instance.is_some() {
            columns.push("instance".to_string());
            values.push(format!("{:?}", self.instance.as_ref().unwrap()));
        }
        if self.job.is_some() {
            columns.push(self.job.as_ref().unwrap().to_string());
            values.push(format!("{:?}", self.instance.as_ref().unwrap()));
        }
        for (label, value) in sample.labels.iter() {
            if !schema.iter().any(|(name, _)| name == label) {
                continue;
            }
            columns.push(label.to_string());
            values.push(format!("{:?}", value));
        }
        columns.push("value".to_string());
        values.push(sample.value.to_string());

        let columns = columns.join(", ");
        let values = values.join(", ");
        format!("INSERT INTO {:?} ({}) VALUES ({})", table, columns, values)
    }

  fn process_metric_family(&mut self, timestamp_millis: u64, family: &MetricFamily) -> bool {
        // TODO: Wrap this in a transaction.
        let mut sql = Vec::new();
        let new_schema = self.schema_for(family);
        let merged_schema = match self.schemas.get(family.var.unwrap()) {
            None => {
                // TODO: Does table already exist?
                sql.push(self.create_table(family.var.unwrap(), &new_schema));
                &new_schema
            }
            Some(current_schema) => {
                // TODO: merge schemas
                current_schema
            }
        };
        match family.r#type {
            SampleType::Counter | SampleType::Gauge | SampleType::Untyped => {
                for sample in &family.samples {
                    sql.push(self.insert_vanilla_samples(
                        timestamp_millis,
                        sample,
                        family.var.unwrap(),
                        merged_schema,
                    ));
                }
            }
            SampleType::Summary => {
              // TODO
            }
            SampleType::Histogram => {
              // TODO
            }
        }
        let sql = sql.join("; ");
        match self.connection.execute(sql) {
            Ok(_) => true,
            Err(err) => {
                error!("SQL error: {}", err);
                false
            }
        }
    }

    pub fn write(&mut self, timestamp_millis: u64, exposition: &str) -> bool {
      let start_marker = Instant::now();
        match parse(exposition) {
            None => false,
            Some(families) => {
              let parse_time = start_marker.elapsed();
              info!("parse time: {:?}", parse_time);
                let mut result = true;
                for family in families {
                    if !self.process_metric_family(timestamp_millis, &family) {
                        result = false;
                    }
                }
                let write_time = start_marker.elapsed();
                info!("write time: {:?}", write_time - parse_time);
                result
            }
        }
    }
}
