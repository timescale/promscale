# 🐳 Docker

## 🔧 Running Docker

A docker image for the Promscale Connector is available
on Docker Hub at [timescale/promscale](https://hub.docker.com/r/timescale/promscale/).

A docker image of TimescaleDB with the `promscale`
extension is available at on Docker Hub at
[`timescaledev/promscale-extension:latest-pg12`](https://hub.docker.com/r/timescaledev/promscale-extension).

## 🕞 Setting up cron jobs

Docker installations also need to make sure the `execute_maintenance()`
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

The docker image is configured either through CLI flags or environment variables.
environment variables are prefixed with `TS_PROM`.

The list of available cli flags is available in [here](/docs/cli.md) in
our docs or by running with the `-h` flag (e.g. `docker run timescale/promscale -h`).

## 🛠 Building from source

You can build the Docker container using the [Dockerfile](/Dockerfile)