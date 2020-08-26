# 🔟 Binaries

## 🔧 Installing pre-built binaries

You can download pre-built binaries for the Timescale-Prometheus
Connector [on our release page](/releases).

We recommend installing the [timescale_prometheus_extra](/extension)
PostgreSQL extension into the TimescaleDB database you are connecting to.
Instructions on how to compile and install the extension are in the
extensions [README](/extension/Readme.md). While this isn't a requirement, it
does optimize certain queries.
Please note that the extension requires Postgres version 12 of newer.

## 🕞 Setting up cron jobs

Binary installations also need to make sure the `execute_maintenance()`
procedure on a regular basis (e.g. via cron). We recommend executing it every
30 minutes. This is necessary to execute maintenance tasks such as enforcing
data retention policies according to the configured policy.

## 🔥 Configuring Prometheus to use this remote storage connector

You must tell prometheus to use this remote storage connector by adding
the following lines to `prometheus.yml`:
```
remote_write:
  - url: "http://<connector-address>:9201/write"
remote_read:
  - url: "http://<connector-address>:9201/read"
```

## ⚙️ Configuration

The Timescale-Prometheus Connector binary is configured either through
CLI flags or environment variables. The list of all available flags is
displayed on the help `timescale-prometheus -h` command. All
environment variables are prefixed with `TS_PROM`.

## 🛠 Building from source

Before building, make sure the following prerequisites are installed:

* [Go](https://golang.org/dl/)

The Timescale-Prometheus Connector is a Go project managed by go
modules. You can download it in
any directory and on the first build it will download it's required
dependencies.

```bash
# Fetch the source code of Timescale-Prometheus in any directory
$ git clone git@github.com:timescale/timescale-prometheus.git
$ cd ./timescale-prometheus

# Install the Timescale-Prometheus Connector binary (will automatically detect and download)
# dependencies.
$ cd cmd/timescale-prometheus
$ go install

# Building without installing will also fetch the required dependencies
$ go build ./...
```