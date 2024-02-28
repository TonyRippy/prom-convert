# Collect Prometheus Data into SQLite

To be clear, this is a terrible idea.

At the moment I am using this to explore different ways to represent monitoring
data in the relational model, and to play around with the new 
[Stanchion](https://github.com/dgllghr/stanchion) column-store extension.

## Build Instructions

You can build the tool itself from source using `cargo build --release`.

If you want to use the Stanchion SQLite extension, you will need to build that
separately following the directions from Stanchion project.

## How to Use

```
Collects data from Prometheus clients and stores it locally in SQLite.

Usage: prom2sqlite [OPTIONS] <TARGET> <OUTPUT>

Arguments:
  <TARGET>  The URL of a Prometheus client endpoint to scrape. If "-", then read from stdin
  <OUTPUT>  The path to the SQLite database file to store metrics

Options:
      --host <HOST>            The IP address to listen on for connections. Only needed when running as a server [default: 127.0.0.1]
  -p, --port <PORT>            The port number to use. Only needed when running as a server [default: 8080]
      --instance <INSTANCE>    
      --job <JOB>              
  -i, --interval <INTERVAL>    How often metrics will be scraped, in seconds [default: 5]
  -b, --buffer <BUFFER>        How many scrapes to hold in memory before dropping samples [default: 5]
      --stanchion <STANCHION>  Path to the Stanchion SQLite extension
  -h, --help                   Print help
  -V, --version                Print version
```

There are several ways to use this tool to scrape monitoring data and collect
it into a self contained SQLite3 database:

### Read from Stdin

It is possible to generate or download the Prometheus exposition format using
other tools and pipe it into `prom2sqlite`. To do this, you will need to pass 
in "`-`" as the target. For example:

```shell
curl -s http://localhost:9100/metrics | prom2sqlite - out.db
```

### Collect from Live Process

If you specify a URL as the target, then the tool will regularly scrape
Prometheus data from that URL until the tool is terminated. You can control how
often the process is sampled using the `--interval` flag. Example:

```shell
prom2sqlite --interval=10 http://localhost:9100/metrics out.db
```

### Output as Database

The tool takes a second required parameter that specifies where the collected
data should be written. If the file does not already exist, then it will
create a new SQLite3 database that uses a [known schema](src/schema.sql). If
the file already exists, it assumes that it also uses this schema and inserts
new data into the existing tables. These databases use the normal SQLite 
row-based storage.

#### Column Store?

That said, monitoring data can often be efficiently stored using column-based
formats. There is an experimental SQLite extension named 
[Stanchion](https://github.com/dgllghr/stanchion) that adds support for 
column-based storage to SQLite. 

If you would like to use the Stanchion SQLite extension, you will need to provide
a path to the extension's compiled library. For example:

```shell
prom2sqlite --stanchion=$HOME/stanchion/zig-out/lib/libstanchion http://localhost:9100/metrics out.db
```

Since this changes the on-disk format, you will also need to use the extension
when querying the data later.