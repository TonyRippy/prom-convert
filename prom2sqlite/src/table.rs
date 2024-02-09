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

use parse::{parse, LabelSet, MetricFamily, SampleType};
use sqlite::{BindableWithIndex, State};
use std::collections::HashMap;
use std::time::Instant;

const PRELUDE_SQL: &str = include_str!("./prelude.sql");

pub enum ColumnType {
    String,
    Integer,
    Float,
}

pub struct TableWriter {
    connection: sqlite::Connection,
    instance: Option<i64>,
    job: Option<i64>,
    metric_cache: HashMap<String, i64>,
    label_cache: HashMap<String, i64>,
    label_value_cache: HashMap<(i64, String), i64>,
    series_cache: HashMap<(i64, Vec<i64>), i64>,
}

impl TableWriter {
    pub fn open(database: &str) -> sqlite::Result<TableWriter> {
        let connection = sqlite::open(database)?;
        connection.execute(PRELUDE_SQL)?;
        Ok(TableWriter {
            connection,
            instance: None,
            job: None,
            metric_cache: HashMap::new(),
            label_cache: HashMap::new(),
            label_value_cache: HashMap::new(),
            series_cache: HashMap::new(),
        })
    }

    pub fn set_instance(&mut self, instance: &str) -> sqlite::Result<()> {
        self.instance = Some(self.get_label_value_cached("instance", instance)?);
        Ok(())
    }

    pub fn set_job(&mut self, job: &str) -> sqlite::Result<()> {
        self.job = Some(self.get_label_value_cached("job", job)?);
        Ok(())
    }

    fn create_scalar(&self, var: &str, value_type: ColumnType) -> sqlite::Result<()> {
        let sql = format!(
            "CREATE TABLE {:?} (
                timestamp DATETIME NOT NULL,
                series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
                value {} NOT NULL);",
            var,
            match value_type {
                ColumnType::String => "TEXT",
                ColumnType::Integer => "INTEGER",
                ColumnType::Float => "REAL",
            }
        );
        self.connection.execute(sql)
    }

    fn get_metric_id(&self, family: &MetricFamily) -> sqlite::Result<i64> {
        // TODO: Wrap in a transaction?
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM metric WHERE name = ?")?;
        stmt.bind((1, family.var.unwrap()))?;
        if State::Row == stmt.next()? {
            return stmt.read(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO metric (name, type, help) VALUES (?, ?, ?) RETURNING id")?;
        stmt.bind((1, family.var.unwrap()))?;
        stmt.bind((
            2,
            match family.r#type {
                SampleType::Counter => "counter",
                SampleType::Gauge => "gauge",
                SampleType::Untyped => "untyped",
                SampleType::Summary => "summary",
                SampleType::Histogram => "histogram",
            },
        ))?;
        stmt.bind((3, family.help))?;
        let id = match stmt.next()? {
            State::Row => stmt.read(0)?,
            State::Done => unreachable!(),
        };
        // Create a timeseries table for the metric.
        match family.r#type {
            SampleType::Counter | SampleType::Gauge => {
                self.create_scalar(family.var.unwrap(), ColumnType::Float)?
            }
            SampleType::Untyped => self.create_scalar(family.var.unwrap(), ColumnType::String)?,
            SampleType::Summary => {
                // TODO:  implement summary table creation
            }
            SampleType::Histogram => {
                // TODO: implement histogram table creation
            }
        }
        Ok(id)
    }

    fn get_metric_id_cached(&mut self, family: &MetricFamily) -> sqlite::Result<i64> {
        if let Some(id) = self.metric_cache.get(family.var.unwrap()) {
            return Ok(*id);
        }
        let id = self.get_metric_id(family)?;
        self.metric_cache
            .insert(family.var.unwrap().to_string(), id);
        Ok(id)
    }

    fn get_label_id(&self, label: &str) -> sqlite::Result<i64> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM label WHERE name = ?")?;
        stmt.bind((1, label))?;
        if State::Row == stmt.next()? {
            return stmt.read(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO label (name) VALUES (?) RETURNING id")?;
        stmt.bind((1, label))?;
        match stmt.next()? {
            State::Row => stmt.read(0),
            State::Done => unreachable!(),
        }
    }

    fn get_label_id_cached(&mut self, label: &str) -> sqlite::Result<i64> {
        if let Some(id) = self.label_cache.get(label) {
            return Ok(*id);
        }
        let id = self.get_label_id(label)?;
        self.label_cache.insert(label.to_string(), id);
        Ok(id)
    }

    fn get_label_value(&mut self, label_id: i64, value: &str) -> sqlite::Result<i64> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM label_value WHERE label_id = ? AND value = ?")?;
        stmt.bind((1, label_id))?;
        stmt.bind((2, value))?;
        if State::Row == stmt.next()? {
            return stmt.read(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO label_value (label_id, value) VALUES (?, ?) RETURNING id")?;
        stmt.bind((1, label_id))?;
        stmt.bind((2, value))?;
        match stmt.next()? {
            State::Row => stmt.read(0),
            State::Done => unreachable!(),
        }
    }

    fn get_label_value_cached(&mut self, label: &str, value: &str) -> sqlite::Result<i64> {
        let label_id = self.get_label_id_cached(label)?;
        let key = (label_id, value.to_string());
        if let Some(id) = self.label_value_cache.get(&key) {
            return Ok(*id);
        }
        let id = self.get_label_value(label_id, value)?;
        self.label_value_cache.insert(key, id);
        Ok(id)
    }

    fn get_series_id(&mut self, metric_id: i64, label_value_ids: &[i64]) -> sqlite::Result<i64> {
        // Build a query to find any series that matches those labels
        // BUG: this doesn't verify that it matches *only* these labels!
        let mut sql = vec![format!(
            "SELECT id FROM series WHERE metric_id = {}",
            metric_id
        )];
        for label_value_id in label_value_ids.iter() {
            sql.push(format!(
                "SELECT series_id FROM label_set WHERE label_value_id = {}",
                label_value_id
            ));
        }
        let sql = sql.join(" INTERSECT ");

        let mut stmt = self.connection.prepare(&sql)?;
        if stmt.next()? == State::Row {
            return stmt.read(0);
        }
        // Insert a new series.
        let mut stmt = self
            .connection
            .prepare("INSERT INTO series (metric_id) VALUES (?) RETURNING id")?;
        stmt.bind((1, metric_id))?;
        let series_id = match stmt.next()? {
            State::Row => stmt.read(0)?,
            State::Done => unreachable!(),
        };
        for label_value_id in label_value_ids.iter() {
            let mut stmt = self
                .connection
                .prepare("INSERT INTO label_set (series_id, label_value_id) VALUES (?, ?)")?;
            stmt.bind((1, series_id))?;
            stmt.bind((2, *label_value_id))?;
            match stmt.next()? {
                State::Row => unreachable!(),
                State::Done => {}
            }
        }
        Ok(series_id)
    }

    fn get_series_id_cached(
        &mut self,
        metric_id: i64,
        label_set: &LabelSet,
    ) -> sqlite::Result<i64> {
        let mut label_value_ids = Vec::with_capacity(label_set.len() + 2);
        if let Some(label_value_id) = self.instance {
            label_value_ids.push(label_value_id);
        }
        if let Some(label_value_id) = self.job {
            label_value_ids.push(label_value_id);
        }
        for &(label, value) in label_set {
            let label_value_id = self.get_label_value_cached(label, value)?;
            label_value_ids.push(label_value_id);
        }
        let key = (metric_id, label_value_ids);
        if let Some(id) = self.series_cache.get(&key) {
            return Ok(*id);
        }
        let series_id = self.get_series_id(metric_id, &key.1)?;
        self.series_cache.insert(key, series_id);
        Ok(series_id)
    }

    fn insert_vanilla_sample<T: BindableWithIndex>(
        &self,
        table_name: &str,
        timestamp: &str,
        series_id: i64,
        value: T,
    ) -> sqlite::Result<()> {
        let mut stmt = self.connection.prepare(&format!(
            "INSERT INTO {:?} (timestamp, series_id, value) VALUES (?, ?, ?)",
            table_name
        ))?;
        stmt.bind((1, timestamp))?;
        stmt.bind((2, series_id))?;
        stmt.bind((3, value))?;
        match stmt.next()? {
            State::Done => Ok(()),
            State::Row => unreachable!(),
        }
    }

    fn process_metric_family(&mut self, timestamp_millis: u64, family: &MetricFamily) -> bool {
        // TODO: wrap this in a transaction?
        let timestamp = chrono::DateTime::from_timestamp_millis(timestamp_millis as i64)
            .unwrap()
            .to_rfc3339();
        let metric_id = match self.get_metric_id_cached(family) {
            Ok(id) => id,
            Err(err) => {
                error!("unable to lookup metric family: {}", err);
                return false;
            }
        };
        for sample in &family.samples {
            let series_id = match self.get_series_id_cached(metric_id, &sample.labels) {
                Ok(id) => id,
                Err(err) => {
                    error!(
                        "unable to lookup series for metric {} and labels {:?}: {}",
                        family.var.unwrap(),
                        &sample.labels,
                        err
                    );
                    return false;
                }
            };
            let result = match family.r#type {
                SampleType::Counter | SampleType::Gauge => {
                    let value = match sample.value.parse::<f64>() {
                        Ok(value) => value,
                        Err(err) => {
                            error!("unable to parse gauge value {:?}: {}", sample.value, err);
                            return false;
                        }
                    };
                    self.insert_vanilla_sample(family.var.unwrap(), &timestamp, series_id, value)
                }
                SampleType::Untyped => self.insert_vanilla_sample(
                    family.var.unwrap(),
                    &timestamp,
                    series_id,
                    sample.value,
                ),
                SampleType::Summary => {
                    // TODO
                    Ok(())
                }
                SampleType::Histogram => {
                    // TODO
                    Ok(())
                }
            };
            if let Err(err) = result {
                error!("unable to insert sample: {}", err);
                return false;
            }
        }
        true
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
