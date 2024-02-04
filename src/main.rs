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

use bytes::{Buf, Bytes};
use clap::Parser;
use env_logger::Env;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::Service;
use hyper::{body::Incoming, Response};
use hyper::{server::conn::http1, StatusCode};
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use opentelemetry::global;
use std::future::Future;
use std::io::{Error, Read};
use std::pin::Pin;
use std::process::ExitCode;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::signal;
use tokio::task;
use tokio::time::MissedTickBehavior;

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

    /// The network address of a Prometheus client to scrape.
    target: String,

    /// The path to the SQLite database file to store metrics.
    output: String,
}

const UI_HTML: &str = include_str!("../ui/dist/index.html");
const UI_CSS: &str = include_str!("../ui/dist/css/index.min.css");
const UI_JS: &str = include_str!("../ui/dist/js/index.min.js");

struct Svc {
    // client_config: ClientConfig,
}

impl Svc {
    fn new() -> Self {
        Self {
          // client_config: ClientConfig {
          //     prometheus_urls: vec!["http://localhost:9090".to_string()],
          // },
      }
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
                .body(UI_HTML.into()),
            "/css" => Response::builder()
                .header("Content-Type", "text/css; charset=utf-8")
                .status(StatusCode::OK)
                .body(UI_CSS.into()),
            "/js" => Response::builder()
                .header("Content-Type", "text/javascript; charset=utf-8")
                .status(StatusCode::OK)
                .body(UI_JS.into()),
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

type FetchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn fetch(url: String) -> FetchResult<String> {
    debug!("starting fetch of {}", url);
    let url = url.parse::<Uri>()?;

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

    // TODO: Verify that this decodes string output correctly.
    // This might only work for UTF-8 ecoded data.
    let mut output = String::new();
    let buf = res.collect().await.unwrap().aggregate();
    buf.reader().read_to_string(&mut output)?;

    Ok(output)
}

async fn sample_metrics(url: String) {
    match fetch(url).await {
        Ok(result) => {
            debug!("fetch result: {}", result);
        }
        Err(err) => {
            error!("fetch error: {}", err);
        }
    }
}

async fn monitoring_loop(args: &Args) -> Result<(), Error> {
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
              tokio::spawn(sample_metrics(args.target.to_string()));
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

fn main() -> ExitCode {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    // TODO: Figure out how to use OTel's logging support.
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    debug!("logging configured");

    // Initialize OTel Metrics
    // TODO
    debug!("metrics configured");

    // Initialize OTel Tracing
    // TODO
    debug!("tracing configured");

    let exit_code = match runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .and_then(|rt| rt.block_on(monitoring_loop(&args)))
    {
        Err(err) => {
            error!("{}", err);
            ExitCode::FAILURE
        }
        _ => ExitCode::SUCCESS,
    };

    // Shutdown OTel pipelines
    global::shutdown_tracer_provider();
    global::shutdown_meter_provider();
    // global::shutdown_logger_provider();

    exit_code
}
