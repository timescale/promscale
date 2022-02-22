# Promscale
[![Go](https://github.com/timescale/promscale/workflows/Go/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3AGo)
[![reviewdog - golangci](https://github.com/timescale/promscale/workflows/reviewdog%20-%20golangci/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3A%22reviewdog+-+golangci%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/timescale/promscale)](https://goreportcard.com/report/github.com/timescale/promscale)
[![Code Climate](https://api.codeclimate.com/v1/badges/c6b16c0bbcb0690c9c71/maintainability)](https://codeclimate.com/github/timescale/promscale/maintainability)
[![GoDoc](https://godoc.org/github.com/timescale/promscale?status.svg)](https://pkg.go.dev/github.com/timescale/promscale)

- **[Website](https://www.timescale.com/promscale)**
- **[Slack Community](https://timescaledb.slack.com/)** (join the #promscale channel) 
- **[Forum](https://www.timescale.com/forum/c/promscale-and-observability)**
- **[Blog](https://blog.timescale.com/tag/observability/)** 

<img alt="Promscale" src="images/promscale-logo.png" width="600px">

**Promscale is an open source observability backend for metrics and traces
powered by SQL**.

It's built on the robust and high-peformance foundation of
**PostgreSQL and TimescaleDB**. It has native support for **Prometheus** metrics and
**OpenTelemetry** traces as well as many other formats like StatsD, Jaeger and Zipkin
through the OpenTelemetry Collector and is **100% PromQL compliant**.
It's **full SQL** capabilities enable developers to **correlate metrics, traces and also
business data** to derive new valuable insights not possible when data is siloed in
different systems.

Built on top of PostgreSQL and [TimescaleDB](https://www.timescale.com/) it inherits
**rock-solid reliability, native compression up to 90%, continuous aggregates and the
operational maturity** of a system that is run on millions of instances worldwide.

For Prometheus users, Promscale provides a robust and highly scalable long-term storage
system that is 100% PromQL compliant.
✅  Promscale has consistently been one of the only long-term stores for Prometheus data that continue to maintain its top-performance, receiving
[100% compliance test score](https://promlabs.com/promql-compliance-test-results/2020-12-01/promscale)
each time (with no cross-cutting concerns) from PromLab's
[PromQL Compliance Test Suite](https://promlabs.com/promql-compliance-tests/).

<img src="docs/assets/promscale-arch.png" alt="Promscale Architecture Diagram" width="800"/>

**Tracing** support is currently in beta. Read [the documentation](docs/tracing.md) to get started.

For a detailed description of the initial architecture of Promscale for Prometheus metrics that covers
some key design principles, please see [our design doc][design-doc].

If you have any questions, please join the #promscale channel on
[TimescaleDB Slack](https://slack.timescale.com/).

This repository also contains the source code for **prom-migrator**. **Prom-migrator** is
an **open-source**, **community-driven** and **free-to-use**, **universal prometheus
data migration tool**, that migrates data from one storage system to another, leveraging Prometheus's
remote storage endpoints. For more information about prom-migrator, visit
[prom-migrator's README](https://github.com/timescale/promscale/blob/master/cmd/prom-migrator/README.md).

# Documentation

* **[About TimescaleDB](#-about-timescaledb)**
* **[Timescale cloud](#-timescale-cloud)**
* **[Features](#-features)**
* **[Installation](#-choose-your-own-installation-adventure)**
  * [The Observability Suite for Kubernetes][tobs]
  * [Docker](docs/docker.md)
  * [Binaries](docs/binary.md)
  * [Source](docs/binary.md#building-from-source)
  * [Helm](deploy/helm-chart/README.md)
  * [Tutorial: Bare Metal Setup for a Monitoring Suite](docs/bare-metal-promscale-stack.md)
  * [Configuring Prometheus](docs/configuring_prometheus.md)
* **[Analyzing Data Using SQL](docs/sql_schema.md)**
  * [Data Model](docs/sql_schema.md#data-model-schema)
  * [Filtering Series](docs/sql_schema.md#filtering-series)
  * [Data Retention](docs/sql_schema.md#data-retention)
  * [Roles and Permissions](docs/sql_permissions.md)
* **[Tutorial: Getting Started with Promscale](https://docs.timescale.com/timescaledb/latest/tutorials/promscale/)**
* **[Tracing (beta)](docs/tracing.md)**
* **[High Availability](docs/high-availability/prometheus-HA.md)**
* **[Multi-Node TimescaleDB](docs/multinode.md)**
* **[Multi-tenancy](docs/multi_tenancy.md)**
* **[Writing Metric Data](docs/writing_to_promscale.md)**
* **[Alerting](docs/alerting.md)**
  * [Alerting for Promscale](docs/mixin/README.md)
* **[Downsampling](docs/downsampling.md)**
* **[Deleting Data](docs/metric_deletion_and_retention.md)**
* **[Quick Tips](#-quick-tips)**
* **[FAQ](docs/faq.md)**
* **[Contributing](#%EF%B8%8F-contributing)**


## 🐯 About TimescaleDB

**[TimescaleDB](https://github.com/timescale/timescaledb)** is a
**distributed time-series database built on PostgreSQL** that scales to
over 10 million of metrics per second, supports native compression,
handles high cardinality, and offers native time-series capabilities,
such as data retention policies, continuous aggregate views,
downsampling, data gap-filling and interpolation.

TimescaleDB also supports full SQL, a variety of data types (numerics,
text, arrays, JSON, booleans), and ACID semantics. Operationally mature
capabilities include high-availability, streaming backups, upgrades over
time, roles and permissions, and security.

TimescaleDB has a **large and active user community** (tens of millions
of downloads, hundreds of thousands of active deployments, Slack channel
with 7,000+ members). Users include Comcast, Fujitsu,
Schneider Electric, Siemens, Walmart, Warner Music, and thousands of
others.

Developers and organizations around the world trust TimescaleDB with their
time-series data. AppDynamics (now part of Cisco Systems and one of the
largest application performance monitoring providers) relies on TimescaleDB
as its main metrics database. TimescaleDB is also the preferred (recommended)
backend datasource for Zabbix users and is natively supported in Grafana.

## 🚀 Timescale Cloud

**[Timescale cloud](https://www.timescale.com/cloud)** offers you an unrivalled experience of TimescaleDB.
All you have to do to integrate with Timescale cloud is configure the db uri using `-db-uri` flag in Promscale to database
instance running in Timescale cloud.

**Note:** Timescale cloud offers a 30-day free trial.  

## 🌟 Features

Click the video below for an overview of Promscale:

<p align="center">
<a href="https://www.youtube.com/watch?v=FWZju1De5lc&list=PLsceB9ac9MHTrmU-q7WCEvies-o7"> <img src="https://media.giphy.com/media/aLIuNJwflN425UvYjR/giphy.gif"> </a>
</p>

* **Analysis in PromQL and SQL** get the ability to analyze data in both query languages. Use PromQL for monitoring and alerting and
  SQL for deeper analytics and compatibility with a huge ecosystem of data visualization, analysis, and AI/ML tools.
* **Rock-solid stability** due to being built on top of PostgreSQL, with 30+ years of development work.
* **Support for backfilling** to ingest data from the past.
* **Native support for Multi-Tenancy**. Ingest data from multiple tenants and write queries across [more than one tenant](docs/multi_tenancy.md)
  easily, using PromQL or SQL.
* **High-Availability** support for [Prometheus HA deployments](docs/high-availability/prometheus-HA.md) as well as
  high-availability deployments of [TimescaleDB itself](https://blog.timescale.com/blog/high-availability-timescaledb-postgresql-patroni-a4572264a831/).
* **Simple architecture**. Unlike some other long-term stores, our architecture consists of only three components: Prometheus, Promscale, and TimescaleDB.
* **ACID compliance** to ensure consistency of your data.
* **Horizontal scalability** using [multinode support](https://blog.timescale.com/blog/timescaledb-2-0-a-multi-node-petabyte-scale-completely-free-relational-database-for-time-series/) with TimescaleDB version 2.0.
* **Operationally mature**. Built on top of Postgres, data written via Promscale is safe, reliable and offers
  all of the advantages of an operationally mature platform.

## 🔧 Choose your own (installation) adventure

We have four main ways to set up Promscale:

#### tobs (recommended for Kubernetes environments)

[The Observability Suite for Kubernetes][tobs] is a
CLI tool and Helm chart that makes installing a full observability suite into your
Kubernetes cluster really easy. Tobs includes Prometheus, TimescaleDB,
Promscale Connector, and Grafana.

To get started, run the following in your terminal, then follow the on-screen instructions.

```bash
curl --proto '=https' --tlsv1.2 -sSLf  https://tsdb.co/install-tobs-sh |sh
```

Or visit the [tobs GitHub repo][tobs] for more information and instructions
on advanced configuration.

#### 🐳 Docker

We provide [docker images](https://github.com/timescale/promscale/releases) with every release.

Instructions on how to use our docker images are available [here](docs/docker.md).

#### 🔟 Binaries

We have [pre-packaged binaries](https://github.com/timescale/promscale/releases) available for MacOS and Linux on both the x86_64 and i386 architectures.
Instructions on how to use our prepackaged binaries are available [here](docs/binary.md).

You can also [build binaries from source](docs/binary.md#building-from-source).

#### ⎈ Helm (sub)chart for Promscale Connector only

A Helm chart for only the Promscale Connector is available in
the [helm-chart directory](deploy/helm-chart/README.md) of this repository.

This is used as a Helm dependency from the `tobs`
[Helm chart](https://github.com/timescale/tobs/tree/master/chart) and can be used as a dependency in your own custom Helm chart as well.

## 🔍 Analyzing the data using SQL

We describe how to use our pre-defined views and functions to work with the prometheus data in [the SQL schema doc](docs/sql_schema.md).

A Reference for our SQL API is [available here](docs/sql_api.md).

## 💡 Quick tips

### Configuring Data Retention

By default, data is stored for 90 days and then deleted. This
default can be changed globally or overridden for individual
metrics.

If using tobs, these settings can be changed using the
`tobs metrics retention` command (use
`tobs metrics retention -h` to see all options).

These setting can also be changed using the appropriate
[SQL commands](docs/sql_schema.md#data-retention).

### 🌐 Prometheus HTTP API

The Promscale Connector can be used directly as a Prometheus Data
Source in Grafana, or other software.

The connector implements some endpoints of the currently stable (V1) [Prometheus HTTP
API](https://prometheus.io/docs/prometheus/latest/querying/api). The API is
accessible at `http://<promscale_connector_address>:9201/api/v1` and can be
used to execute instant or range PromQL queries against the data in
TimescaleDB, as well as retrieve the metadata for series, label names and
label values.

A Reference for the implemented endpoints of the Prometheus HTTP API is [available here](docs/prometheus_api.md)

### Configuring Prometheus to filter which metrics are sent (optional)

You can limit the metrics being sent to the adapter (and thus being
stored in your long-term storage) by setting up `write_relabel_configs`
in Prometheus, via the `prometheus.yml` file.
Doing this can reduce the amount of space used by your database and thus
increase query performance.

The example below drops all metrics starting with the prefix `go_`,
which matches Golang process information exposed by exporters like
`node_exporter`:

```
remote_write:
 - url: "http://promscale_connector:9201/write"
   write_relabel_configs:
      - source_labels: [__name__]
        regex: 'go_.*'
        action: drop
```

Additional information about setting up relabel configs, the
`source_labels` field, and the possible actions can be found in the
[Prometheus Docs](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

## ✏️ Contributing

We welcome contributions to the Promscale Connector, which is
licensed and released under the open-source Apache License, Version 2.
The same [Contributor's
Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/promscale)
(CLA) if you're a new contributor.

[design-doc]: https://tsdb.co/prom-design-doc
[tobs]: https://github.com/timescale/tobs
