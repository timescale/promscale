# Changelog
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

We use the following categories for changes:
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Unreleased]

### Added
- Add support to instrument Promscale's Otel GRPC server with Prometheus metrics [#1061]

### Fixed
- Fix broken `promscale_packager` telemetry field for docker envs [#1077]

## [0.8.0] - 2022-01-18

### Added
- Allow templating host and uri connection strings in helm chart [#1055]
- Add ability to configure the default chunk interval on startup [#991]
- Add `ps_trace.delete_all_traces()` function to delete all trace data [#1012]
- Add `ps_trace.set_trace_retention_period(INTERVAL)` function to set trace retention period [#1015]
- Add `ps_trace.get_trace_retention_period()` database function to get current trace retention period [#1015]
- Add ability to set additional environment variables in helm chart [#1041]
- Add OpenTelemetry tracing instrumentation to metric ingest codepath

### Changed
- Rename CLI flags to improve user interface [#964]
- BREAKING: Enable and configure 30 day default retention period for span data [#1015]
- BREAKING: The `promscale_query_batch_duration_seconds` metric was renamed to `promscale_metrics_query_remote_read_batch_duration_seconds`
  to clarify what it represents. [#1040]

### Deprecated
- Deprecate `migrate` flag [#964]

### Removed
- Remove deprecated `-promql-enable-feature` flag [#964]
- Remove deprecated leader election [#964]
- Remove obsoleted jaeger-query-proxy

### Fixed
- helm-charts: use fixed target port on svc-promscale [#1009]
- Fix passing of async flag [#1008]
- Remove the event_name_check constraint [#979]
- Fix _prom_catalog.metric_view() function on non-tsdb installs [#958]

## [0.7.1] - 2021-12-03

### Fixed
- Fix upgrade scripts

## [0.7.0] - 2021-12-02

### Added
- Beta OpenTelemetry Tracing support
- `-enable-feature` cli flag
- Support for Postgres 14
- helm-chart: tracing support can be enabled with `openTelemetry.enable`
- helm-chart: include ServiceAccount definition
- helm-chart: add ServiceMonitor definition

### Changed
- helm-chart: `args` section renamed to `extraArgs`
- helm-chart: `tracing` section renamed to `openTelemetry`
- helm-chart: improve UX of service configuration
- helm-chart: move connection details into a secret
- helm-chart: use stringData to store Secret values

### Deprecated
- The `-promql-enable-feature` cli flag has been superseded by `-enable-feature`

### Removed
- Remove deprecated TS_PROM_ prefixed env-var support
- Remove unused `db-connect-retries` cli flag

### Fixed
- Fixed a memory leak when using the series endpoint (GET/POST /api/v1/series)
- helm-chart: allow numbers to be passed as connection parameters
- helm-chart: fix incorrect annotation setting when prometheus scrape is disabled
