# Promscale

[![Go](https://github.com/timescale/promscale/workflows/Go/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3AGo)
[![reviewdog - golangci](https://github.com/timescale/promscale/workflows/reviewdog%20-%20golangci/badge.svg)](https://github.com/timescale/promscale/actions?query=workflow%3A%22reviewdog+-+golangci%22)
[![Go Report Card](https://goreportcard.com/badge/github.com/timescale/promscale)](https://goreportcard.com/report/github.com/timescale/promscale)
[![Code Climate](https://api.codeclimate.com/v1/badges/c6b16c0bbcb0690c9c71/maintainability)]()
[![GoDoc](https://godoc.org/github.com/timescale/promscale?status.svg)](https://pkg.go.dev/github.com/timescale/promscale)

- **[Website](https://www.timescale.com/promscale)*
- **[Install](https://docs.timescale.com/promscale/latest/installation/)*
- **[Docs](https://docs.timescale.com/promscale/latest/)**
- **[Slack Community](https://timescaledb.slack.com/)** (join the #promscale channel)
- **[Forum](https://www.timescale.com/forum/c/promscale-and-observability)**
- **[Blog](https://blog.timescale.com/tag/observability/)**

<img alt="Promscale" src="images/promscale-logo.png" width="600px">

**Promscale is a unified observability backend for metrics and traces
powered by SQL and built on PostgreSQL and TimescaleDB**.

It has native support for **Prometheus** metrics and
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
✅ Promscale has consistently been one of the only long-term stores for Prometheus data that continue to maintain its top-performance, receiving
[100% compliance test score](https://promlabs.com/promql-compliance-test-results/2020-12-01/promscale)
each time (with no cross-cutting concerns) from PromLab's
[PromQL Compliance Test Suite](https://promlabs.com/promql-compliance-tests/).

For OpenTelemetry, Jaeger and Zipkin users, Promscale provides the ability to run deep analysis
on traces using SQL. Install our lightweight [opentelemetry-demo](https://github.com/timescale/opentelemetry-demo)
on your computer to quickly get a feel of what's possible. More details in
[this blog post](https://www.timescale.com/blog/learn-opentelemetry-tracing-with-this-lightweight-microservices-demo/).

<img src="docs/assets/promscale-arch.png" alt="Promscale Architecture Diagram" width="800"/>

This repository contains the source code of the Promscale Connector. Promscale also requires that the Promscale Extension
who lives in this repository is installed in the database. The extension sets up and manages the database schemas
and provides performance and SQL query experience improvements.

For a detailed description of the initial architecture of Promscale for Prometheus metrics that covers
some key design principles, please see [our design doc](https://tsdb.co/prom-design-doc).

If you have any questions, please join the #promscale channel on
[TimescaleDB Slack](https://slack.timescale.com/).

This repository also contains the source code for **prom-migrator**. **Prom-migrator** is
an **open-source**, **community-driven** and **free-to-use**, **universal prometheus
data migration tool**, that migrates data from one storage system to another, leveraging Prometheus's
remote storage endpoints. For more information about prom-migrator, visit
[prom-migrator's README](https://github.com/timescale/promscale/blob/master/migration-tool/cmd/prom-migrator/README.md).

You may also want to check [tobs](https://github.com/timescale/tobs) which makes it very easy to deploy a complete
observability stack built on Prometheus, OpenTelemetry and Promscale in Kubernetes via cli or helm.

# Documentation

Complete user documentation is available in https://docs.timescale.com/promscale/latest/

## ✎ Contributing

We welcome contributions to the Promscale Connector, which is
licensed and released under the open-source Apache License, Version 2.
The same [Contributor's
Agreement](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/promscale)
(CLA) if you're a new contributor.
