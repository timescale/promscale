# Prometheus remote storage adapter for PostgreSQL

With this remote storage adapter, Prometheus can use PostgreSQL as a long-term store for time-series metrics. The adapter currently requires the `pg_prometheus` extension for PostgreSQL and optionally supports [TimescaleDB](https://github.com/timescale/timescaledb) for better performance and scalability.

## Building

Before building, make sure the following prerequisites are installed:

* [Glide](https://glide.sh/) for dependency management.
* [Go](https://golang.org/dl/)

Then build as follows:

```bash
# Install dependencies (only required once)
glide install

# Build binary
make

# Build Docker image
make docker

# Push to Docker registry
make push
```
