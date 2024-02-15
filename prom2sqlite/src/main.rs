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
use std::time::{Duration, SystemTime};

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
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::MissedTickBehavior;

use driver::fetch::fetch;

mod table;
use table::TableWriter;

const INDEX_HTML: &str = include_str!("./index.html");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The IP address to listen on for connections.
    /// Only needed when running as a server.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// The port number to use.
    /// Only needed when running as a server.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    // The instance label to use for all samples.
    // If not provided, the address of the source URL will be used.
    #[arg(long)]
    instance: Option<String>,

    // The job label to use for all samples.
    // If not provided, the address of the source URL will be used.
    #[arg(long)]
    job: Option<String>,

    /// How often metrics will be scraped, in seconds.
    #[arg(short, long, default_value_t = 5)]
    interval: u64,

    /// How many scrapes to hold in memory before dropping samples.
    #[arg(short, long, default_value_t = 5)]
    buffer: usize,

    /// The URL of a Prometheus client endpoint to scrape.
    /// If "-", then read from stdin.
    source: String,

    /// The path to the SQLite database file to store metrics.
    target: String,
}

struct Svc {}

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

async fn collect(url: Uri, tx: Sender<(u64, String)>) {
    debug!("collecting sample");
    match fetch(url).await {
        Ok((timestamp_millis, exposition)) => {
            debug!("collected sample {}", timestamp_millis);
            if let Err(err) = tx.try_send((timestamp_millis, exposition)) {
                error!("unable to send sample {}: {}", timestamp_millis, err);
            }
        }
        Err(err) => error!("unable to collect sample: {}", err),
    }
}

async fn polling_loop(host: String, port: u16, interval: u64, url: Uri, tx: Sender<(u64, String)>) {
    let listener = match TcpListener::bind((host.as_str(), port)).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("error binding to {}:{}: {}", host.as_str(), port, err);
            return;
        }
    };
    info!("listening on {}:{}", host.as_str(), port);

    let mut sample_interval = tokio::time::interval(Duration::from_secs(interval));
    sample_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("Interrupt signal received.");
                break
            }
            _ = sample_interval.tick() => {
              debug!("scheduling sample");
              tokio::spawn(collect(url.clone(), tx.clone()));
            }
            Ok((tcp_stream, _)) = listener.accept() => {
              tokio::spawn(
                  http1::Builder::new()
                      .keep_alive(false)
                      .serve_connection(TokioIo::new(tcp_stream), Svc{}));
          }
        }
    }
}

fn read_from_stdin(tx: Sender<(u64, String)>) -> ExitCode {
    let mut input = String::new();
    if let Err(err) = std::io::stdin().read_to_string(&mut input) {
        error!("error reading from stdin: {}", err);
        return ExitCode::FAILURE;
    }
    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    if let Err(err) = tx.try_send((timestamp, input)) {
        error!("unable to send sample: {}", err);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

async fn writer_loop(mut rx: Receiver<(u64, String)>, mut writer: TableWriter) {
    debug!("writer started");
    loop {
        match rx.recv().await {
            Some((timestamp_millis, exposition)) => {
                debug!("processing sample {}", timestamp_millis);
                writer.write(timestamp_millis, &exposition);
                debug!("processing done");
            }
            None => {
                debug!("no more samples to process");
                break;
            }
        }
    }
}

async fn run(args: Args) -> Result<ExitCode, Error> {
    let uri = match args.source.as_str() {
        "-" => None,
        uri => match uri.parse::<Uri>() {
            Ok(uri) => Some(uri),
            Err(err) => {
                error!("invalid URI {}: {}", uri, err);
                return Ok(ExitCode::FAILURE);
            }
        },
    };

    let mut writer = match TableWriter::open(&args.target) {
        Ok(writer) => writer,
        Err(err) => {
            error!("error opening database: {}", err);
            return Ok(ExitCode::FAILURE);
        }
    };
    if let Some(instance) = args.instance.as_ref() {
        if let Err(err) =  writer.set_instance(instance) {
            warn!("unable to set \"instance\" label to \"{}\": {}", instance, err);
        }
    } else if let Some(authority) = uri
        .as_ref()
        .and_then(|url| url.authority())
        .map(|f| f.as_str())
    {
        if let Err(err) =  writer.set_instance(authority) {
            warn!("unable to set \"instance\" label to \"{}\": {}", authority, err);
        }
    }
    if let Some(job) = args.job.as_ref() {
        if let Err(err) =  writer.set_job(job) {
            warn!("unable to set \"job\" label to \"{}\": {}", job, err);
        }
    }

    let (tx, rx) = channel::<(u64, String)>(args.buffer);
    let writer_task = tokio::spawn(writer_loop(rx, writer));

    let exit_code = match uri {
        None => read_from_stdin(tx),
        Some(uri) => {
            debug!("starting polling loop");
            polling_loop(args.host, args.port, args.interval, uri, tx).await;
            ExitCode::SUCCESS
        }
    };
    debug!("waiting for writer task to complete");
    writer_task.await?;
    debug!("done");
    Ok(exit_code)
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

    let exit_code = match runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .and_then(|rt| rt.block_on(run(args)))
    {
        Ok(exit_code) => exit_code,
        Err(err) => {
            error!("error running application thead: {}", err);
            ExitCode::FAILURE
        }
    };

    // Shutdown OTel pipelines
    global::shutdown_tracer_provider();
    global::shutdown_meter_provider();
    // global::shutdown_logger_provider();

    exit_code
}
