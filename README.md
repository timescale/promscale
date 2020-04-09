# Timescale-Prometheus Connector

With this remote storage connector, Prometheus can use TimescaleDB as a compressed, long-term store for time-series metrics.
For a detailed description of this architecture, please see [our design doc][design-doc].

# Quick start with Helm (recommended)

The quickest way to get started is by using the Helm chart from
[timescale-observability](https://github.com/timescale/timescale-observability),
which provides a broader framework for installing an end-to-end monitoring
stack.

The following command will install Prometheus, TimescaleDB, Timescale-Prometheus Connector, and Grafana
into your Kubernetes cluster:
```
helm repo add timescale https://charts.timescale.com/
helm repo update
helm install <release_name> timescale/timescale-observability
```

# Configuring Helm Chart

To get a fully-documented configuration file for `timescale-observability`, please run:

```
helm show values timescale/timescale-observability > my_values.yml
```

You can then edit `my_values.yml` and deploy the release with the following command:

```
helm upgrade --install <release_name> --values my_values.yml timescale/timescale-observability
```

By default, the `timescale-observability` Helm chart sets up a single-instance of TimescaleDB; if you are
interested in a replicated setup for high-availabilty with automated backups, please see
[this github repo](https://github.com/timescale/timescaledb-kubernetes/tree/master/charts/timescaledb-single) for additional instructions.

# Advanced

## Helm (sub)chart for Timescale-Prometheus Connector only

A Helm chart for only the Timescale-Prometheus Connector is available in the [helm-chart directory](helm-chart/README.md) of this repository.

This is used as a dependency from the `timescale-observability` Helm chart and can be used as a dependency in your own custom Helm chart.

## Non-Helm installation methods

### Binaries

You can download pre-built binaries for the Timescale-Prometheus Connector [on our release page](https://github.com/timescale/timescale-prometheus/releases).

### Docker

A docker image for the Timescale-Prometheus Connector is available
on Docker Hub at [timescale/timescale-prometheus](https://hub.docker.com/r/timescale/timescale-prometheus/).

## Non-Helm Configuration

### Configuring Prometheus to use this remote storage connector

You must tell prometheus to use this remote storage connector by adding the
following lines to `prometheus.yml`:
```
remote_write:
  - url: "http://<connector-address>:9201/write"
remote_read:
  - url: "http://<connector-address>:9201/read"
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

The Timescale-Prometheus Connector is a Go project managed by go modules. You can download it in
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

You can build the Docker container using the [Dockerfile](Dockerfile).

## Contributing

We welcome contributions to the Timescale-Prometheus Connector, which is
licensed and released under the open-source Apache License, Version 2.  The
same [Contributor's
Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/timescale-prometheus) (CLA) if
you're a new contributor.

[design-doc]: https://tsdb.co/prom-design-doc
