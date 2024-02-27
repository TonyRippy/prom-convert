// SQL-like abstractions used as an intermediate representation.
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

use driver::parse::{parse, LabelSet, MetricFamily, SampleType};
use rusqlite::{Connection, LoadExtensionGuard};
use std::collections::HashMap;
use std::time::Instant;

const PRELUDE_SQL: &str = include_str!("./schema.sql");

pub struct TableWriter {
    connection: Connection,
    use_stanchion: bool,
    instance: Option<i64>,
    job: Option<i64>,
    metric_cache: HashMap<String, i64>,
    label_cache: HashMap<String, i64>,
    label_value_cache: HashMap<(i64, String), i64>,
    series_cache: HashMap<(i64, Vec<i64>), i64>,
}

impl TableWriter {
    pub fn open(database: &str, stanchion: Option<&str>) -> rusqlite::Result<TableWriter> {
        info!("using sqlite version {}", rusqlite::version());
        let connection = Connection::open(database)?;
        if let Some(stanchion) = stanchion {
            info!("using stanchion from {}", stanchion);
            unsafe {
                let _guard = LoadExtensionGuard::new(&connection)?;
                connection.load_extension(stanchion, None)?;
            }
        }
        connection.execute_batch(PRELUDE_SQL)?;
        Ok(TableWriter {
            connection,
            use_stanchion: stanchion.is_some(),
            instance: None,
            job: None,
            metric_cache: HashMap::new(),
            label_cache: HashMap::new(),
            label_value_cache: HashMap::new(),
            series_cache: HashMap::new(),
        })
    }

    pub fn set_instance(&mut self, instance: &str) -> rusqlite::Result<()> {
        self.instance = Some(self.get_label_value_cached("instance", instance)?);
        Ok(())
    }

    pub fn set_job(&mut self, job: &str) -> rusqlite::Result<()> {
        self.job = Some(self.get_label_value_cached("job", job)?);
        Ok(())
    }

    fn create_scalar(&self, table_name: &str) -> rusqlite::Result<()> {
        let sql = if self.use_stanchion {
            format!(
                "CREATE VIRTUAL TABLE {:?} USING stanchion (
                    series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
                    timestamp INTEGER NOT NULL,
                    value REAL NOT NULL,
                    SORT KEY (series_id, timestamp)
            );",
                table_name
            )
        } else {
            format!(
                "CREATE TABLE {:?} (
                    series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
                    timestamp DATETIME NOT NULL,
                    value REAL NOT NULL,
                    PRIMARY KEY (series_id, timestamp)
            );",
                table_name
            )
        };
        self.connection.execute(&sql, ())?;
        Ok(())
    }

    fn insert_scalar(
        &self,
        table_name: &str,
        timestamp_millis: u64,
        series_id: i64,
        value: f64,
    ) -> rusqlite::Result<()> {
        let mut stmt = self.connection.prepare(&format!(
            "INSERT INTO {:?} (series_id, timestamp, value) VALUES (?1, ?2, ?3)",
            table_name
        ))?;
        if self.use_stanchion {
            stmt.insert((series_id, timestamp_millis, value))?;
        } else {
            stmt.insert((
                series_id,
                chrono::DateTime::from_timestamp_millis(timestamp_millis as i64)
                    .unwrap()
                    .to_rfc3339(),
                value,
            ))?;
        }
        Ok(())
    }

    fn get_metric_id(&self, family: &MetricFamily) -> rusqlite::Result<i64> {
        // TODO: Wrap in a transaction?
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM metric WHERE name = ?1")?;
        let mut rows = stmt.query((family.var.unwrap(),))?;
        if let Some(row) = rows.next()? {
            return row.get(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO metric (name, type, help) VALUES (?1, ?2, ?3) RETURNING id")?;
        let mut rows = stmt.query((
            family.var.unwrap(),
            match family.r#type {
                SampleType::Counter => "counter",
                SampleType::Gauge => "gauge",
                SampleType::Untyped => "untyped",
                SampleType::Summary => "summary",
                SampleType::Histogram => "histogram",
            },
            family.help,
        ))?;
        let id = match rows.next()? {
            Some(row) => row.get(0)?,
            None => unreachable!(),
        };
        // Create a timeseries table for the metric.
        match family.r#type {
            SampleType::Counter | SampleType::Gauge | SampleType::Untyped => {
                self.create_scalar(family.var.unwrap())?
            }
            SampleType::Summary => {
                // TODO:  implement summary table creation
            }
            SampleType::Histogram => {
                // TODO: implement histogram table creation
            }
        }
        Ok(id)
    }

    fn get_metric_id_cached(&mut self, family: &MetricFamily) -> rusqlite::Result<i64> {
        if let Some(id) = self.metric_cache.get(family.var.unwrap()) {
            return Ok(*id);
        }
        let id = self.get_metric_id(family)?;
        self.metric_cache
            .insert(family.var.unwrap().to_string(), id);
        Ok(id)
    }

    fn get_label_id(&self, label: &str) -> rusqlite::Result<i64> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM label WHERE name = ?1")?;
        let mut rows = stmt.query((label,))?;
        if let Some(row) = rows.next()? {
            return row.get(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO label (name) VALUES (?1) RETURNING id")?;
        let mut rows = stmt.query((label,))?;
        match rows.next()? {
            Some(row) => row.get(0),
            None => unreachable!(),
        }
    }

    fn get_label_id_cached(&mut self, label: &str) -> rusqlite::Result<i64> {
        if let Some(id) = self.label_cache.get(label) {
            return Ok(*id);
        }
        let id = self.get_label_id(label)?;
        self.label_cache.insert(label.to_string(), id);
        Ok(id)
    }

    fn get_label_value(&mut self, label_id: i64, value: &str) -> rusqlite::Result<i64> {
        let mut stmt = self
            .connection
            .prepare("SELECT id FROM label_value WHERE label_id = ?1 AND value = ?2")?;
        let mut rows = stmt.query((label_id, value))?;
        if let Some(row) = rows.next()? {
            return row.get(0);
        }
        let mut stmt = self
            .connection
            .prepare("INSERT INTO label_value (label_id, value) VALUES (?1, ?2) RETURNING id")?;
        let mut rows = stmt.query((label_id, value))?;
        match rows.next()? {
            Some(row) => row.get(0),
            None => unreachable!(),
        }
    }

    fn get_label_value_cached(&mut self, label: &str, value: &str) -> rusqlite::Result<i64> {
        let label_id = self.get_label_id_cached(label)?;
        let key = (label_id, value.to_string());
        if let Some(id) = self.label_value_cache.get(&key) {
            return Ok(*id);
        }
        let id = self.get_label_value(label_id, value)?;
        self.label_value_cache.insert(key, id);
        Ok(id)
    }

    fn get_series_id(&mut self, metric_id: i64, label_value_ids: &[i64]) -> rusqlite::Result<i64> {
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
        let mut rows = stmt.query(())?;
        if let Some(row) = rows.next()? {
            return row.get(0);
        }

        // Insert a new series.
        let mut stmt = self
            .connection
            .prepare("INSERT INTO series (metric_id) VALUES (?1) RETURNING id")?;
        let mut rows = stmt.query((metric_id,))?;
        let series_id = match rows.next()? {
            Some(row) => row.get(0)?,
            None => unreachable!(),
        };
        let mut stmt = self
            .connection
            .prepare("INSERT INTO label_set (series_id, label_value_id) VALUES (?1, ?2)")?;
        for label_value_id in label_value_ids.iter() {
            stmt.insert((series_id, *label_value_id))?;
        }
        Ok(series_id)
    }

    fn get_series_id_cached(
        &mut self,
        metric_id: i64,
        label_set: &LabelSet,
    ) -> rusqlite::Result<i64> {
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

    fn process_metric_family(&mut self, timestamp_millis: u64, family: &MetricFamily) -> bool {
        // TODO: wrap this in a transaction?
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
                SampleType::Counter | SampleType::Gauge | SampleType::Untyped => {
                    let value = match sample.value.parse::<f64>() {
                        Ok(value) => value,
                        Err(err) => {
                            error!("unable to parse scalar value {:?}: {}", sample.value, err);
                            return false;
                        }
                    };
                    self.insert_scalar(family.var.unwrap(), timestamp_millis, series_id, value)
                }
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
