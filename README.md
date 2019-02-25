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
 timescale/prometheus-postgresql-adapter:master \
 -pg.host=pg_prometheus \
 -pg.prometheus-log-samples
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

## Building

Before building, make sure the following prerequisites are installed:

* [Dep](https://golang.github.io/dep/) for dependency management.
* [Go](https://golang.org/dl/)

Then build as follows:

```bash
# Install dependencies (only required once)
dep ensure

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
