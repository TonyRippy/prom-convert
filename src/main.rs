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

use clap::Parser;
use env_logger::Env;

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

fn main() {
    // Parse command-line arguments
    let args = Args::parse();

    // Initialize logging
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    info!("Hello, world!");
}
