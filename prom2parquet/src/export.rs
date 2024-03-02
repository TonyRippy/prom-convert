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

use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use driver::parse::{MetricFamily, Sample, SampleType};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

struct RecordBatchBuilder {
    pub schema: Arc<Schema>,
    name_builder: StringBuilder,
    labels_builder: MapBuilder<StringBuilder, StringBuilder>,
    timestamp_builder: TimestampMillisecondBuilder,
    value_builder: Float64Builder,
}

impl RecordBatchBuilder {
    fn new() -> Self {
        // Define schema
        let timestamp_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_string().into())),
            false,
        );

        let kv_struct = DataType::Struct(Fields::from(vec![
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
        ]));

        let var_field = Field::new("metric", DataType::Utf8, false);
        let labels_field = Field::new(
            "labels",
            DataType::Map(Arc::new(Field::new("entries", kv_struct, false)), false),
            false,
        );
        let scalar_field = Field::new("scalar", DataType::Float64, true);

        let schema = Arc::new(Schema::new(vec![
            timestamp_field,
            var_field,
            labels_field,
            scalar_field,
        ]));

        let name_builder = StringBuilder::new();
        let labels_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let timestamp_builder = TimestampMillisecondBuilder::new().with_timezone("UTC");
        let value_builder = Float64Builder::new();

        Self {
            schema,
            name_builder,
            labels_builder,
            timestamp_builder,
            value_builder,
        }
    }

    fn append_scalar(&mut self, timestamp: i64, sample: &Sample) -> bool {
        self.timestamp_builder.append_value(timestamp);
        self.name_builder.append_value(sample.var);
        for (key, value) in sample.labels.iter() {
            self.labels_builder.keys().append_value(key);
            self.labels_builder.values().append_value(value);
        }
        self.labels_builder.append(true).unwrap();
        match sample.value.parse::<f64>() {
            Ok(value) => {
                self.value_builder.append_value(value);
                true
            }
            Err(err) => {
                error!("unable to parse scalar value {:?}: {}", sample.value, err);
                self.value_builder.append_null();
                false
            }
        }
    }

    fn finish(&mut self) -> RecordBatch {
        let timestamp = self.timestamp_builder.finish();
        let name = self.name_builder.finish();
        let labels = self.labels_builder.finish();
        let value = self.value_builder.finish();

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(timestamp),
                Arc::new(name),
                Arc::new(labels),
                Arc::new(value),
            ],
        )
        .unwrap()
    }
}

pub struct ParquetExporter {
    writer: ArrowWriter<File>,
    builder: RecordBatchBuilder,
}

impl ParquetExporter {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let builder = RecordBatchBuilder::new();

        let file = std::fs::File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let writer = ArrowWriter::try_new(file, builder.schema.clone(), Some(props))?;
        Ok(Self { writer, builder })
    }
}

impl driver::Exporter for ParquetExporter {
    fn export(&mut self, timestamp_millis: u64, family: &MetricFamily) -> bool {
        for sample in family.samples.iter() {
            if !self.builder.append_scalar(timestamp_millis as i64, sample) {
                return false;
            }
        }
        let record_batch = self.builder.finish();
        match self.writer.write(&record_batch) {
            Ok(_) => true,
            Err(err) => {
                error!("unable to write record batch: {}", err);
                false
            }
        }
    }

    fn close(&mut self) {
        // This is *seriously* hacky, but it's the only way to close the ArrowWriter 
        // behind a mutable reference. 
        // TODO: Keep working on this, and try to find a better way to finalize writes at the end.
        let new_writer = match ArrowWriter::try_new(
            File::open("/dev/null").unwrap(),
            self.builder.schema.clone(),
            None,
        ) {
            Ok(writer) => writer,
            Err(err) => {
                error!("unable to create dummy Parquet writer: {}", err);
                return;
            }
        };
        let old_writer = std::mem::replace(&mut self.writer, new_writer);
        if let Err(err) = old_writer.close() {
            error!("unable to close Parquet writer: {}", err);
        }
    }
}
