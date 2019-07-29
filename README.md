# Prometheus remote storage adapter for PostgreSQL

With this remote storage adapter, Prometheus can use PostgreSQL as a long-term store for time-series metrics.

Related packages to install:
- [pg_prometheus extension for PostgreSQL](https://github.com/timescale/pg_prometheus) (required)
- [TimescaleDB](https://github.com/timescale/timescaledb) (optional
for better performance and scalability)

## Quick start

You can download pre-built binaries [here](https://github.com/timescale/prometheus-postgresql-adapter/releases)

## Docker instructions

A docker image for the prometheus-postgreSQL storage adapter is available
on Docker Hub at [timescale/prometheus-postgresql-adapter](https://hub.docker.com/r/timescale/prometheus-postgresql-adapter/).

The easiest way to use this image is in conjunction with the `pg_prometheus`
docker [image](https://hub.docker.com/r/timescale/pg_prometheus/) provided by Timescale.
This image packages PostgreSQL, `pg_prometheus`, and TimescaleDB together in one
docker image.

To run this image use:
```
docker run --name pg_prometheus -d -p 5432:5432 timescale/pg_prometheus:latest postgres \
      -csynchronous_commit=off
```

Then, start the prometheus-postgreSQL storage adapter using:
```
 docker run --name prometheus_postgresql_adapter --link pg_prometheus -d -p 9201:9201 \
 timescale/prometheus-postgresql-adapter:latest \
 -pg-host=pg_prometheus \
 -pg-prometheus-log-samples
```

Finally, you can start Prometheus with:
```
docker run -p 9090:9090 --link prometheus_postgresql_adapter -v /path/to/prometheus.yml:/etc/prometheus/prometheus.yml \
       prom/prometheus
```
(a sample `prometheus.yml` file can be found in `sample-docker-prometheus.yml` in this repository).

## Configuring Prometheus to use this remote storage adapter

You must tell prometheus to use this remote storage adapter by adding the
following lines to `prometheus.yml`:
```
remote_write:
  - url: "http://<adapter-address>:9201/write"
remote_read:
  - url: "http://<adapter-address>:9201/read"
```

## Environment variables

All of the CLI flags are also available as environment variables, and begin with the prefix `TS_PROM`.
For example, the following mappings apply:

```
-adapter-send-timeout => TS_PROM_ADAPTER_SEND_TIMEOUT
-leader-election-rest => TS_PROM_LEADER_ELECTION_REST
-pg-host              => TS_PROM_PG_HOST
-web-telemetry-path   => TS_PROM_WEB_TELEMETRY_PATH
...
```

Each CLI flag and equivalent environment variable is also displayed on the help `prometheus-postgresql-adapter -h` command.

## Configuring Prometheus to filter which metrics are sent

You can limit the metrics being sent to the adapter (and thus being stored in your long-term storage) by 
setting up `write_relabel_configs` in Prometheus, via the `prometheus.yml` file.
Doing this can reduce the amount of space used by your database and thus increase query performance. 

The example below drops all metrics starting with the prefix `go_`, which matches Golang process information
exposed by exporters like `node_exporter`:

```
remote_write:
 - url: "http://prometheus_postgresql_adapter:9201/write"
   write_relabel_configs:
      - source_labels: [__name__]
        regex: 'go_.*'
        action: drop
```

Additional information about setting up relabel configs, the `source_labels` field, and the possible actions can be found in the [Prometheus Docs](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

## Building

Before building, make sure the following prerequisites are installed:

* [Go](https://golang.org/dl/)

Then build as follows:

```bash

# Build binary
make
```

## Building new Docker images

```bash

# Build Docker image
make docker-image

# Push to Docker registry (requires permission)
make docker-push ORGANIZATION=myorg
```

## Contributing

We welcome contributions to this adaptor, which like TimescaleDB is released under the Apache2 Open Source License.  The same [Contributors Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md) applies; please sign the [Contributor License Agreement](https://cla-assistant.io/timescale/prometheus-postgresql-adapter) (CLA) if you're a new contributor.
