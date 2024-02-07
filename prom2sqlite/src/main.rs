// prom2sqlite -- Collect Prometheus data and store locally to SQLite
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

use std::future::Future;
use std::io::{Error, Read};
use std::pin::Pin;
use std::process::ExitCode;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use env_logger::Env;
use http_body_util::Full;
use hyper::service::Service;
use hyper::{body::Incoming, Response};
use hyper::{server::conn::http1, StatusCode};
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use opentelemetry::global;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::MeterProvider;
use prometheus::{Encoder, TextEncoder};
use tokio::net::TcpListener;
use tokio::runtime;
use tokio::signal;
use tokio::task;
use tokio::time::MissedTickBehavior;

use fetch::{fetch, parse, MetricFamily};

const INDEX_HTML: &str = include_str!("./index.html");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// How often metrics will be collected, in seconds.
    #[arg(short, long, default_value_t = 5)]
    interval: u64,

    /// The URL of a Prometheus client endpoint to scrape.
    /// If "-", then read from stdin.
    source: String,

    /// The path to the SQLite database file to store metrics.
    target: String,
}


struct Svc {}

impl Svc {
    fn new() -> Self {
        Self {}
    }
}

impl Service<Request<Incoming>> for Svc {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::http::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let res = match req.uri().path() {
            "/" => Response::builder()
                .header("Content-Type", "text/html; charset=utf-8")
                .status(StatusCode::OK)
                .body(INDEX_HTML.into()),
            "/metrics" => {
                let encoder = TextEncoder::new();
                let metric_families = prometheus::gather();
                let mut buffer = vec![];
                encoder.encode(&metric_families, &mut buffer).unwrap();
                Response::builder()
                    .header("Content-Type", encoder.format_type())
                    .status(StatusCode::OK)
                    .body(buffer.into())
            }
            "/-/healthy" => Response::builder().status(StatusCode::OK).body("OK".into()),
            "/-/ready" => Response::builder().status(StatusCode::OK).body("OK".into()),
            // "/-/reload" => Response::builder()
            //     .status(StatusCode::NOT_IMPLEMENTED)
            //     .body(Full::default()),
            "/-/quit" => Response::builder()
                .status(StatusCode::NOT_IMPLEMENTED)
                .body(Full::default()),
            _ => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::default()),
        };
        Box::pin(async { res })
    }
}

async fn poll<F>(url: Uri, process: &'static F)
where
    F: Fn(&str) -> bool,
{
    match fetch(url).await {
        Ok(result) => { _ = process(&result); }
        Err(err) => error!("fetch error: {}", err)
    }
}

async fn polling_loop<F>(args: &Args, source: Uri, target: &'static F) -> Result<(), Error>
where
    F: Fn(&str) -> bool + Send + Sync,
{
    let listener = TcpListener::bind((args.host.as_str(), args.port)).await?;
    info!("Listening on {}:{}", args.host.as_str(), &args.port);

    let mut sample_interval = tokio::time::interval(Duration::from_secs(args.interval));
    sample_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Interrupt signal received.");
                break
            }
            _ = sample_interval.tick() => {
              debug!("scheduling sample");
              tokio::spawn(poll(source.clone(), target));
            }
            Ok((tcp_stream, _)) = listener.accept() => {
              tokio::spawn(
                  http1::Builder::new()
                      .keep_alive(false)
                      .serve_connection(TokioIo::new(tcp_stream), Svc::new()));
          }

        }
        task::yield_now().await;
    }
    Ok(())
}

fn read_from_stdin<F>(target: &F) -> ExitCode
where
    F: Fn(&str) -> bool,
{
    let mut input = String::new();
    if let Err(err) = std::io::stdin().read_to_string(&mut input) {
        error!("error reading from stdin: {}", err);
        return ExitCode::FAILURE;
    }
    if target(&input) {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn log_families(input: &str) -> bool {
    match parse(input) {
        Some(families) => {
            for family in families {
                debug!("family: {:?}", family);
            }
            true
        }
        None => false,
    }
}

fn main() -> ExitCode {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    // TODO: Figure out how to use OTel's logging support.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    debug!("logging configured");

    // Initialize OTel Metrics
    // TODO

    // configure OpenTelemetry to use this registry
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(prometheus::default_registry().clone())
        .build()
        .unwrap();
    // set up a meter meter to create instruments
    let provider = MeterProvider::builder().with_reader(exporter).build();
    let meter = provider.meter("prom2sqlite");
    debug!("metrics configured");

    // Initialize OTel Tracing
    // TODO
    debug!("tracing configured");

    let exit_code = match args.source.as_str() {
        "-" => read_from_stdin(&log_families),
        uri => match uri.parse() {
            Err(err) => {
                error!("invalid URL {}: {}", uri, err);
                ExitCode::FAILURE
            }
            Ok(uri) => match runtime::Builder::new_current_thread()
                .enable_time()
                .enable_io()
                .build()
                .and_then(|rt| rt.block_on(polling_loop(&args, uri, &log_families)))
            {
                Err(err) => {
                    error!("{}", err);
                    ExitCode::FAILURE
                }
                _ => ExitCode::SUCCESS,
            },
        },
    };

    // Shutdown OTel pipelines
    global::shutdown_tracer_provider();
    global::shutdown_meter_provider();
    // global::shutdown_logger_provider();

    exit_code
}
