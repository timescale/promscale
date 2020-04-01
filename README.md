# Prometheus remote storage connector for TimescaleDB

With this remote storage adapter, Prometheus can use TimescaleDB as a compressed,long-term store for time-series metrics.
A detailed description and design doc for this project can be found at TODO.

# Quick start with Helm (recommended)

The Quickest way to get started is by using the Helm chart from [timescale-observability](https://github.com/timescale/timescale-observability).

The following command will install Prometheus, TimescaleDB, Timescale-Prometheus Connector, and Grafana
into your Kubernetes cluster:
```
helm install <release_name> timescale/timescale-observability
```

# Configuring Helm Chart

To get a fully-documented configuration file for timescale-observability please run

```
helm show values timescale/timescale-observability > my_values.yml
```

You can then edit my_values.yml and deploy the release with the following command

```
helm upgrade --install <release_name> --values my_values.yml timescale/timescale-observability
```

Additional information about how to set up backup/restore and high-availability of the TimescaleDB
database is available at TODO.

# Advanced

## Helm (sub)chart for Timescale-Prometheus Connector only

A Helm chart for only the Timescale-Prometheus Connector is available in the [helm-chart directory](helm-chart/README.md) of this repository. This is used as a dependency from the timescale-observability helm chart and can be used as a dependency in your own
custom helm chart.

## Non-Helm installation methods

### Binaries

You can download pre-built binaries for the Timescale-Prometheus Connector [on our release page](https://github.com/timescale/timescale-prometheus/releases).

### Docker

A docker image for the Timescale-Prometheus Connector is available
on Docker Hub at [timescale/timescale-prometheus](https://hub.docker.com/r/timescale/timescale-prometheus/).

## Non-Helm Configuration

### Configuring Prometheus to use this remote storage adapter

You must tell prometheus to use this remote storage adapter by adding the
following lines to `prometheus.yml`:
```
remote_write:
  - url: "http://<adapter-address>:9201/write"
remote_read:
  - url: "http://<adapter-address>:9201/read"
```

### Configuring Prometheus to filter which metrics are sent (optional)

You can limit the metrics being sent to the adapter (and thus being stored in your long-term storage) by
setting up `write_relabel_configs` in Prometheus, via the `prometheus.yml` file.
Doing this can reduce the amount of space used by your database and thus increase query performance.

The example below drops all metrics starting with the prefix `go_`, which matches Golang process information
exposed by exporters like `node_exporter`:

```
remote_write:
 - url: "http://timescale_prometheus_connector:9201/write"
   write_relabel_configs:
      - source_labels: [__name__]
        regex: 'go_.*'
        action: drop
```

Additional information about setting up relabel configs, the `source_labels` field, and the possible actions can be found in the [Prometheus Docs](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

### Configuring the Timescale-Prometheus Connector Binary

The Timescale-Prometheus Connector Binary is configured either through CLI flags or environment variables.
The list of all available flags is displayed on the help `timescale-prometheus -h` command. All
environment variables are prefixed with `TS_PROM`.

## Building

Before building, make sure the following prerequisites are installed:

* [Go](https://golang.org/dl/)

Timescale-Prometheus is a Go project managed by go modules. You can download it in
any directory and on the first build it will download it's required dependencies.

```bash
# Fetch the source code of Outflux in any directory
$ git clone git@github.com:timescale/timescale-prometheus.git
$ cd ./timescale-prometheus

# Install the Timescale-Prometheus Connector binary (will automatically detect and download)
# dependencies.
$ cd cmd/timescale-prometheus
$ go install

# Building without installing will also fetch the required dependencies
$ go build ./...
```

You can build the Docker container using the [Dockerfile](Dockerfile)

## Contributing

We welcome contributions to this adaptor, which like TimescaleDB is released under the Apache2 Open Source License.  The same [Contributors Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md) applies; please sign the [Contributor License Agreement](https://cla-assistant.io/timescale/timeascale-prometheus) (CLA) if you're a new contributor.
