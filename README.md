# Prometheus Conversion Tools

This repo contains tools that allow one to collect data from Prometheus clients
and store the data into a variety of self-contained formats.
The formats supported so far are:

* [SQLite3](prom2sqlite)

Coming soon:

* Parquet
* OpenTelementry protobufs

## Why Do This?

The primary motivation is to have a lightweight and deterministic way to
collect data from processes that run in system integration tests as part of a
CI/CD pipeline. I've often wished that I had monitoring scrapes from test runs.
This can be extremely useful when trying to track down intermittent test 
failures in complex system tests. These sorts of failures are rarely 
repeatable.

For the data to be useful it would be need to be stored in a format that makes
inspection and analysis easy. As such, the formats supported here can be read
and analyzed with common off-the-shelf tools.

## Project Status

At the moment these tools are exploratory; I'm building them to give myself
experience with these formats. They are not really meant for production use at
the moment. That said, if you find them useful, please let me know! I'm happy
to get them into better shape if needed.