# 🔟 Binaries

## 🔧 Installing pre-built binaries

You can download pre-built binaries for the Promscale
Connector [on our release page](https://github.com/timescale/promscale/releases).

We recommend installing the [promscale](https://github.com/timescale/promscale_extension/releases)
PostgreSQL extension into the TimescaleDB database you are connecting to.
Instructions on how to compile and install the extension are in the
extensions [README](https://github.com/timescale/promscale_extension/blob/master/Readme.md). While this isn't a requirement, it
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

The Promscale Connector binary is configured either through
CLI flags or environment variables. All environment variables are
prefixed with `TS_PROM`.


The list of available cli flags is available in [here](/docs/cli.md) in
our docs or by running with the `-h` flag (e.g. `promscale -h`)

## 🛠 Building from source

Before building, make sure the following prerequisites are installed:

* [Go](https://golang.org/dl/)

The Promscale Connector is a Go project managed by go
modules. You can download it in
any directory and on the first build it will download it's required
dependencies.

```bash
# Fetch the source code of Promscale in any directory
$ git clone git@github.com:timescale/promscale.git
$ cd ./promscale

# Install the Promscale Connector binary (will automatically detect and download)
# dependencies.
$ cd cmd/promscale
$ go install

# Building without installing will also fetch the required dependencies
$ go build ./...
```
