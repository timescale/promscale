# Promscale

[![Go](https://github.com/timescale/promscale/workflows/Go/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3AGo)
[![reviewdog - golangci](https://github.com/timescale/promscale/workflows/reviewdog%20-%20golangci/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3A%22reviewdog+-+golangci%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/timescale/promscale)](https://goreportcard.com/report/github.com/timescale/promscale)
[![Code Climate](https://api.codeclimate.com/v1/badges/c6b16c0bbcb0690c9c71/maintainability)]()
[![GoDoc](https://godoc.org/github.com/timescale/promscale?status.svg)](https://pkg.go.dev/github.com/timescale/promscale)

- **[Website](https://www.timescale.com/promscale)**
- **[Install](https://docs.timescale.com/promscale/latest/installation/)**
- **[Docs](https://docs.timescale.com/promscale/latest/)**
- **[Slack Community](https://timescaledb.slack.com/)** (join the #promscale channel)
- **[Forum](https://www.timescale.com/forum/c/promscale-and-observability)**
- **[Blog](https://blog.timescale.com/tag/observability/)**

<img alt="Promscale" src="docs/assets/promscale-logo.png" width="600px">

Promscale is a unified metric and trace storage backend for Prometheus,
Jaeger and OpenTelemetry built on PostgreSQL and TimescaleDB.

Promscale serves as as robust and 100% PromQL-compliant Prometheus remote storage
and as a durable and scalable Jaeger storage backend.

Unlike other storage backends for observability data, it has a very simple
and easy to manage architecture with just two components: the Promscale
Connector and the Promscale Database.

[Learn more](https://docs.timescale.com/promscale/latest/about-promscale/#promscale-schema-for-metric-data) 
about the Promscale architecture and how it works.

<img src="docs/assets/promscale-arch.png" alt="Promscale Architecture Diagram" width="800"/>

## Promscale for Prometheus

Promscale solves two main Prometheus use cases:

* **Efficient long-term trend analysis and capacity planning**<br/>
Use Promscale as a durable long-term storage for Prometheus metrics. Reduce costs and
improve query performance by leveraging [metric downsampling](https://docs.timescale.com/promscale/latest/downsample-data/)
and [per-metric retention](https://docs.timescale.com/promscale/latest/manage-data/retention/#configure-data-retention-for-metrics) to 
only keep the data you need for as long as you need it. 

* **Single-pane-of-glass across all your Kubernetes clusters**
Use Promscale as a centralized storage for all your Prometheus instances
so you can easily query data across all of them in Grafana and centralize
[alert management](https://docs.timescale.com/promscale/latest/alert/) and
[recording rules](https://docs.timescale.com/promscale/latest/downsample-data/recording/).

**Key features**: 100% PromQL-compliant, high availability, multi-tenancy, PromQL alerting and recording rules, per-metric retention.

If you use Prometheus and are also familiar with PostgreSQL, then Promscale is a great choice for
your Prometheus remote storage. You can scale to millions of series and hundreds of thousands of samples
on a single PostgreSQL node thanks to TimescaleDB.

To get started:
1. [Install Promscale](https://docs.timescale.com/promscale/latest/installation/#install-promscale-with-instrumentation).
2. [Configure Prometheus](https://docs.timescale.com/promscale/latest/send-data/prometheus/) to send data to Promscale.
3. [Configure Grafana](https://docs.timescale.com/promscale/latest/visualize-data/grafana/) to query and visualize metrics from Promscale
using a PromQL and/or a PostgreSQL datasource.

## Promscale for Jaeger and OpenTelemetry

Promscale can natively ingest OpenTelemetry traces and it can also ingest Jaeger and Zipkin traces via
the OpenTelemetry Collector.

Promscale solves three main use cases for Jaeger and OpenTelemetry users:

* **Easy-to-use durable and scalable storage for traces**<br/>
Most users run Jaeger with the in memory or badger storage because the two options for a more durable storage
(Elasticsearch and Cassandra) are very hard to set up and operate. Promscale uses a much simpler architecture
based on PostgreSQL which many developers are comfortable with and scales to 100s of thousands of spans per
second on a single PostgreSQL node.

* **Service performance analysis**<br/>
Because Promcsale can store both metrics and traces, you can use the new 
[Service Performance Management](https://www.jaegertracing.io/docs/1.36/spm/) feature in Jaeger with Promscale 
as the only storage backend for the entire experience.
Promscale also includes a fully customizable, out-of-the-box, and modern
[Application Performance Management (APM) experience](https://docs.timescale.com/promscale/latest/visualize-data/apm-experience/)
in Grafana using SQL queries on OpenTelemetry traces.

* **Trace analysis**<br/>
Jaeger searching capabilities are limited to filtering individual traces. This is helpful when troubleshooting problems once you know
what you are looking for. With Promscale you can use SQL to interrogate your trace data in any way you want and discover issues
that would take you a long time to figure out. 

**Key features:** native OTLP support, SQL queries, APM capabilities, data compression, data retention 

**Try it out** by installing our lightweight [opentelemetry-demo](https://github.com/timescale/opentelemetry-demo). Check
[this blog post](https://www.timescale.com/blog/learn-opentelemetry-tracing-with-this-lightweight-microservices-demo/) for more details.

To get started:
1. [Install Promscale](https://docs.timescale.com/promscale/latest/installation/#install-promscale-with-instrumentation).
2. [Send traces to Promscale](https://docs.timescale.com/promscale/latest/send-data/) in OpenTelemetry, Jaeger or Zipkin format
3. [Configure Jaeger](https://docs.timescale.com/promscale/latest/visualize-data/jaeger/) to query and visualize traces from Promscale.

Also consider:

4. [Configuring Grafana](https://docs.timescale.com/promscale/latest/visualize-data/grafana) to query and visualize traces from Promscale
using a Jaeger and a PostgreSQL datasource.
5. [Install the APM dashboards](https://docs.timescale.com/promscale/latest/visualize-data/apm-experience/) in Grafana.

## Documentation and Help

Complete user documentation is available at https://docs.timescale.com/promscale/latest/

If you have any questions, please join the #promscale channel on
[TimescaleDB Slack](https://slack.timescale.com/).

## Promscale Repositories

This repository contains the source code of the Promscale Connector. Promscale also requires that the Promscale Extension
which lives in [this repository](https://github.com/timescale/promscale_extension) is installed in the TimescaleDB/PostgreSQL
database. The extension sets up and manages the database schemas and provides performance and SQL query experience improvements.

This repository also contains the source code for **prom-migrator**. **Prom-migrator** is
an **open-source**, **community-driven** and **free-to-use**, **universal prometheus
data migration tool**, that migrates data from one storage system to another, leveraging Prometheus's
remote storage endpoints. For more information about prom-migrator, visit
[prom-migrator's README](https://github.com/timescale/promscale/blob/master/migration-tool/cmd/prom-migrator/README.md).

You may also want to check [tobs](https://github.com/timescale/tobs) which makes it very easy to deploy a complete
observability stack built on Prometheus, OpenTelemetry and Promscale in Kubernetes via cli or helm.

## Contributing

We welcome contributions to the Promscale Connector, which is
licensed and released under the open-source Apache License, Version 2.
The same [Contributor's
Agreement](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/promscale)
(CLA) if you're a new contributor.

