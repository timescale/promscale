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

## Added
- Alerting rules for Promscale. You can find them [here](docs/promscale_alerting.md) [#1181, #1185, #1271]
- Add database status and request metrics [#1185]
- Add database SQL stats as Prometheus metrics. These can be queried under `promscale_sql` namespace [#1193]
- Add alerts for database SQL metrics [#1193]
- Query Jaeger traces directly through Promscale [#1224]
- Additional dataset configuration options via `-startup.dataset.config` flag. Read more [here](docs/dataset.md) [#1276, #1310]

### Changed
- Enable tracing by default [#1213], [#1290]
- The Promscale extension is now required, while the Timescaledb extension remains optional. The minimum Timescaledb version supported is now 2.6.1 [#1132], [#1297]
- Disable running Promscale in HA and read-only simultaneously [#1254]
- Metric tables and views are now owned by prom_admin [#1283]

### Fixed
- Register `promscale_ingest_channel_len_bucket` metric and make it a gauge [#1177]
- Log warning when failing to write response to remote read requests [#1180]
- Fix Promscale running even when some component may fail to start [#1217]
- Fix `promscale_ingest_max_sent_timestamp_milliseconds` metric for tracing [#1270]
- Fix `prom_api.reset_metric_retention_period` on two-step continuous aggregates [#1294]

### Removed
- Remove deprecated flags. More info can be found [here](docs/configuration.md#old-flag-removal-in-version-0.11.0) [#1229]

## [0.10.0] - 2022-02-17

### Added
- Add Prometheus metrics support for Tracing [#1102, #1152]
- Add ingested spans-count to telemetry [#1155]
- Add OTEL collector exporter endpoint support to Promscale tracing telemetry exporter [#1148]
- Add example tracing setup to docker-compose [#1024]

### Changed
- Renamed and refactor Promscale metrics for better consistency. New metrics can be found [here](docs/metrics.md) [#1113]
- Add performance metrics in cache module in Promscale [#1113]

### Fixed
- Spans with `end < start`. `start` and `end` are swapped in this case. [#1096]
- Disable push downs which use `offset`, as they are broken [#1129]
- Aggregate pushdown evaluation [#1098]
- Broken `extraEnv` parameter in helm chart values [#1126]
- Logging success message if extension update failed [#1139]

## [0.9.0] - 2022-02-02

### Added
- Add support to instrument Promscale's Otel GRPC server with Prometheus metrics [#1061]

### Changed
- Optimized series ID creation by caching metric ID [#1062]

### Fixed
- Fix broken `promscale_packager` telemetry field for docker envs [#1077]
- Fix compression of old chunks thus reducing storage requirements [#1081]
- Improved INSERT performance by avoidng ON CONFLICT [#1090]

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
