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

use std::io::{Error, Read};
use std::process::ExitCode;
use std::time::Instant;
use std::time::{Duration, SystemTime};

use hyper::Uri;
use tokio::net::TcpListener;
use tokio::runtime;
use tokio::signal;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::MissedTickBehavior;

pub mod fetch;
pub mod parse;
pub mod http;

pub trait Exporter {
    fn export(&mut self, timestamp_millis: u64, family: &parse::MetricFamily) -> bool;
}

pub trait Args {
    /// The (host, port) address to listen on for connections.
    fn addr(&self) -> (&str, u16);

    fn instance(&self) -> Option<&str>;
    fn job(&self) -> Option<&str>;

    /// How often metrics will be scraped.
    fn interval(&self) -> Duration;

    /// How many scrapes to hold in memory before dropping samples.
    fn buffer(&self) -> usize;

    /// The URL of a Prometheus client endpoint to scrape.
    // /// If "-", then read from stdin.
    fn target(&self) -> &str;
}

async fn collect(url: Uri, tx: Sender<(u64, String)>) {
    debug!("collecting sample");
    match fetch::fetch(url).await {
        Ok((timestamp_millis, exposition)) => {
            debug!("collected sample {}", timestamp_millis);
            if let Err(err) = tx.try_send((timestamp_millis, exposition)) {
                error!("unable to send sample {}: {}", timestamp_millis, err);
            }
        }
        Err(err) => error!("unable to collect sample: {}", err),
    }
}

async fn polling_loop(args: &impl Args, url: Uri, tx: Sender<(u64, String)>) {
    let addr = args.addr();
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            error!("error binding to {}:{}: {}", addr.0, addr.1, err);
            return;
        }
    };
    info!("listening on {}:{}", addr.0, addr.1);

    let mut sample_interval = tokio::time::interval(args.interval());
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
              http::serve(tcp_stream);
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

async fn writer_loop(
    mut rx: Receiver<(u64, String)>,
    instance: Option<String>,
    job: Option<String>,
    mut exporter: Box<dyn Exporter + Send>,
) {
    debug!("writer started");
    loop {
        match rx.recv().await {
            Some((timestamp_millis, exposition)) => {
                debug!("processing sample {}", timestamp_millis);
                let start_marker = Instant::now();
                if let Some(families) =
                    parse::parse(instance.as_deref(), job.as_deref(), &exposition)
                {
                    let parse_time = start_marker.elapsed();
                    info!("parse time: {:?}", parse_time);
                    for family in families {
                        if !exporter.export(timestamp_millis, &family) {
                            error!("unable to export metric family");
                        }
                    }
                    let write_time = start_marker.elapsed();
                    info!("write time: {:?}", write_time - parse_time);
                }
                debug!("processing done");
            }
            None => {
                debug!("no more samples to process");
                break;
            }
        }
    }
}

async fn run_async(
    args: &impl Args,
    exporter: Box<dyn Exporter + Send>,
) -> Result<ExitCode, Error> {
    let uri = match args.target() {
        "-" => None,
        uri => match uri.parse::<Uri>() {
            Ok(uri) => Some(uri),
            Err(err) => {
                error!("invalid URI {}: {}", uri, err);
                return Ok(ExitCode::FAILURE);
            }
        },
    };

    let instance = if let Some(instance) = args.instance() {
        Some(instance)
    } else {
        uri.as_ref()
            .and_then(|url| url.authority())
            .map(|f| f.as_str())
    }
    .map(|s| s.to_string());
    let job = args.job().map(|f| f.to_string());

    let (tx, rx) = channel::<(u64, String)>(args.buffer());
    let writer_task = tokio::spawn(writer_loop(rx, instance, job, exporter));

    let exit_code = match uri {
        None => read_from_stdin(tx),
        Some(uri) => {
            debug!("starting polling loop");
            polling_loop(args, uri, tx).await;
            ExitCode::SUCCESS
        }
    };
    debug!("waiting for writer task to complete");
    writer_task.await?;
    debug!("done");
    Ok(exit_code)
}

pub fn run(args: &impl Args, exporter: Box<dyn Exporter + Send>) -> ExitCode {
    match runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .and_then(|rt| rt.block_on(run_async(args, exporter)))
    {
        Ok(exit_code) => exit_code,
        Err(err) => {
            error!("error running application thead: {}", err);
            ExitCode::FAILURE
        }
    }
}
