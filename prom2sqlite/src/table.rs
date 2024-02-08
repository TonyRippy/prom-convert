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

use fetch::{parse, MetricFamily};
use std::collections::HashMap;

#[derive(PartialEq, Debug)]
pub enum ColumnType {
    String,
    Integer,
    Float,
}

type TableSchema = Vec<(String, ColumnType)>;

pub struct TableWriter {
    connection: sqlite::Connection,
    schemas: HashMap<String, TableSchema>,
}

impl TableWriter {
    pub fn open(database: &str) -> sqlite::Result<TableWriter> {
        Ok(TableWriter {
            connection: sqlite::open(database)?,
            schemas: HashMap::new(),
        })
    }

    fn process_metric_family(&mut self, timestamp_millis: u64, family: &MetricFamily) -> bool {
        // let mut schema = Vec::new();
        // schema.push(("timestamp".to_string(), ColumnType::Integer));
        // for label in &family.labels {
        //     schema.push((label.name.clone(), ColumnType::String));
        // }
        // schema.push(("value".to_string(), ColumnType::Float));
        // schema
        debug!("family: {:?}", family);
        true
    }

    pub fn write(&mut self, timestamp_millis: u64, exposition: &str) -> bool {
        match parse(exposition) {
            None => false,
            Some(families) => {
                let mut result = false;
                for family in families {
                    if !self.process_metric_family(timestamp_millis, &family) {
                        // Remember that a write has failed, but keep going.
                        result = false;
                    }
                }
                result
            }
        }
    }
}
