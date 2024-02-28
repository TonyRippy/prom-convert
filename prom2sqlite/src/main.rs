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

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use env_logger::Env;

mod table;
use table::TableExporter;

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

    /// Path to the Stanchion SQLite extension.
    #[arg(long)]
    stanchion: Option<String>,

    /// The URL of a Prometheus client endpoint to scrape.
    /// If "-", then read from stdin.
    source: String,

    /// The path to the SQLite database file to store metrics.
    target: String,
}

impl driver::Args for Args {
    fn addr(&self) -> (&str, u16) {
        (self.host.as_str(), self.port)
    }

    fn instance(&self) -> Option<&str> {
        self.instance.as_deref()
    }

    fn job(&self) -> Option<&str> {
        self.job.as_deref()
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(self.interval)
    }

    fn buffer(&self) -> usize {
        self.buffer
    }

    fn target(&self) -> &str {
        self.source.as_str()
    }
}

fn main() -> ExitCode {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let writer = Box::new(
        match TableExporter::open(&args.target, args.stanchion.as_deref()) {
            Ok(writer) => writer,
            Err(err) => {
                error!("error opening database: {}", err);
                return ExitCode::FAILURE;
            }
        },
    );
    driver::run(&args, writer)
}
