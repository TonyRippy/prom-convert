# Prometheus Conversion Tools

This repo contains tools that allow one to collect data from Prometheus clients
and store the data into a variety of self-contained formats.
The formats supported so far are:

* [SQLite3](prom2sqlite)

Coming soon:

* Parquet
* OpenTelementry protobufs

At the moment these tools are exploratory; I'm building them to give myself
experience with these formats. They are not really meant for production use at
the moment. That said, if you find them useful, please let me know! I'm happy
to get them into better shape if needed.