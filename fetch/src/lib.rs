// fetch -- Collect & parse data from Prometheus clients
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

#[macro_use]
extern crate log;

use std::io::Read;

use bytes::{Buf, Bytes};
use chrono::{DateTime, Utc};
use http_body_util::{BodyExt, Empty};
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use pest::iterators::Pair;
use pest::Parser;

#[derive(pest_derive::Parser)]
#[grammar = "./prometheus.pest"]
struct PrometheusParser;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub enum SampleType {
    Counter,
    Gauge,
    Histogram,
    Summary,

    #[default]
    Untyped,
}

pub type LabelSet<'a> = Vec<(&'a str, &'a str)>;

#[derive(Clone, Debug)]
pub struct Sample<'a> {
    var: &'a str,
    labels: LabelSet<'a>,
    value: &'a str,
    // TODO: Support exemplars?
    // timestamp: Option<&'a str>,
    // exemplar: Option<Exemplar>,
}

#[derive(Clone, Debug, Default)]
pub struct MetricFamily<'a> {
    pub var: Option<&'a str>,  // TODO: this shouldn't be optional?
    pub help: Option<&'a str>,
    pub r#type: SampleType,
    pub samples: Vec<Sample<'a>>,
}

impl<'a> MetricFamily<'a> {
    fn parse(pair: Pair<'a, Rule>) -> Option<MetricFamily> {
        debug_assert_eq!(pair.as_rule(), Rule::metricfamily);

        let mut metric_family = MetricFamily::default();
        let mut samples: Vec<(&str, Sample)> = Vec::new();

        for child in pair.into_inner() {
            match child.as_rule() {
                Rule::metricdescriptor => {
                    if !samples.is_empty() {
                        error!("Metric Descriptor after samples");
                        return None;
                    }
                    if !metric_family.parse_metric_descriptor(child) {
                        return None;
                    }
                }
                Rule::metric => match Self::parse_sample(child) {
                    Some(sample) => metric_family.samples.push(sample),
                    None => return None,
                },
                _ => unreachable!(),
            }
        }
        Some(metric_family)
    }

    fn parse_metric_descriptor(&mut self, pair: Pair<'a, Rule>) -> bool {
        assert_eq!(pair.as_rule(), Rule::metricdescriptor);
        let mut descriptor = pair.into_inner();
        let descriptor_type = descriptor.next().unwrap();
        let metric_name = descriptor.next().unwrap().as_str();
        match self.var {
            None => {
                self.var = Some(metric_name);
            }
            Some(var) => {
                if metric_name != var {
                    error!(
                        "metric name mismatch: {} {}, expected {}",
                        descriptor_type.as_str(),
                        metric_name,
                        var
                    );
                    return false;
                }
            }
        }
        match descriptor_type.as_rule() {
            Rule::kw_help => {
                if self.help.is_some() {
                    warn!("help for {} already set, overwriting", metric_name);
                }
                self.help = Some(descriptor.next().unwrap().as_str());
            }
            Rule::kw_type => {
                if self.r#type != SampleType::Untyped {
                    warn!("type for {} already set, overwriting", metric_name);
                }
                self.r#type = match descriptor.next().unwrap().as_str() {
                    "counter" => SampleType::Counter,
                    "gauge" => SampleType::Gauge,
                    "histogram" => SampleType::Histogram,
                    "summary" => SampleType::Summary,
                    "untyped" => SampleType::Untyped,
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        true
    }

    fn parse_sample(pair: Pair<'a, Rule>) -> Option<Sample> {
        assert_eq!(pair.as_rule(), Rule::metric);

        let mut descriptor = pair.into_inner();
        let metric_name = descriptor.next().unwrap().as_str();
        let labels = if descriptor.peek().unwrap().as_rule() == Rule::labels {
            parse_labels(descriptor.next().unwrap())
        } else {
            Vec::new()
        };
        let value = descriptor.next().unwrap().as_str();
        Some(Sample {
            var: metric_name,
            labels,
            value,
        })
    }
}

fn parse_labels(pair: Pair<Rule>) -> LabelSet {
    assert_eq!(pair.as_rule(), Rule::labels);
    pair.into_inner()
        .map(|label| {
            let mut inner = label.into_inner();
            let name = inner.next().unwrap().as_str();
            let value = inner.next().unwrap().as_str();
            for extra_pair in inner {
                warn!("unexpected token after label: {:?}", extra_pair);
            }
            (name, value)
        })
        .collect()
}

fn parse_exposition(pair: Pair<Rule>) -> Vec<MetricFamily> {
    assert_eq!(pair.as_rule(), Rule::exposition);
    pair.into_inner().flat_map(|p| match p.as_rule() {
        Rule::metricfamily => MetricFamily::parse(p),
        Rule::EOI => None,
        _ => unreachable!()
    }).collect()
}

pub fn parse(input: &str) -> Option<Vec<MetricFamily>> {
    match PrometheusParser::parse(Rule::exposition, input) {
        Ok(mut iter) => {
            let out = parse_exposition(iter.next().unwrap());
            for extra_pair in iter {
                warn!("unexpected token after exposition: {:?}", extra_pair);
            }
            Some(out)
        }
        Err(err) => {
            error!("parse error: {}", err);
            None
        }
    }
}

pub type FetchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn fetch(url: Uri) -> FetchResult<(u64, String)> {
    debug!("starting fetch of {}", url);
    let authority = url.authority().unwrap();
    let host = authority.host();
    let port = authority.port_u16().unwrap_or(80);

    let stream = TcpStream::connect((host, port)).await?;
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            error!("Connection failed: {:?}", err);
        }
    });
    let path = url.path();
    let req = Request::builder()
        .uri(path)
        .header(hyper::header::HOST, authority.as_str())
        .body(Empty::<Bytes>::new())?;

    let res = sender.send_request(req).await?;

    // TODO: This needs real error handling
    debug!("Response: {}", res.status());
    debug!("Headers: {:#?}\n", res.headers());
    let timestamp = match res.headers().get(hyper::header::DATE) {
        Some(date) => {
            let date = date.to_str().unwrap();
            let date = DateTime::parse_from_rfc2822(date).unwrap();
            date.timestamp_millis()
        }
        None => Utc::now().timestamp_millis(),
    };
    // TODO: parse the Date header and use that as the timestamp for the samples?

    // TODO: Verify that this decodes string output correctly.
    // This might only work for UTF-8 ecoded data.
    let mut output = String::new();
    let buf = res.collect().await.unwrap().aggregate();
    buf.reader().read_to_string(&mut output)?;

    Ok((timestamp as u64, output))
}
