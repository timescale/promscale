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

## Unreleased

### Changed

- COPY commands are executed in a single DB roundtrip instead of two [#1814]

## [0.17.0] - 2023-09-01

### Added
- Add support for PostgreSQL 15
- Alerts from promscale monitoring mixin are groupped also by namespace label [#1714]
- Added a new family of metrics tracking database maintenance jobs durations and failures [#1745]
- Allow config options in file to be set as yaml mappings [#1737]
- Add `startup.dataset` option in the config file for the dataset
  configuration. Supersedes `startup.dataset.config` which accepts a string
  instead of a mapping [#1737]
- Add alert to notify about duplicate sample/metric ingestion. [#1688]
- Add histogram to track number samples/metadata/span sent per request [#1767]

### Changed
- Reduced the verbosity of the logs emitted by the vacuum engine [#1715]
- The vacuum engine now throttles the number of workers used based on the oldest txid from
  the chunks needing freezing [#1761]
- In order to reduce the overall load on the system, some internal database
  metrics won't be collected as often as they used to. None of the affected 
  metrics is expected to change faster than its new collection interval [#1793]
- Aggregate metrics at global level to avoid legend pollution in dashboards [#1800]
- The vacuum engine now looks for compressed chunks missing stats and vacuums these too [#1804]

### Fixed
- Fixing the query behind chunks_uncompressed. The new definition should
  change the baseline value [#1794]

## [0.16.0] - 2022-10-20

### Added
- Integration tests to qualify as certified Jaeger remote storage [#1686]

### Changed

### Fixed
- Fix queries returning no references/links when querying traces that were
  ingested using the native Jaeger API.
- Fix traces queries returning duplicated events and links (or logs and
  references in Jaeger) for traces with more than one event and one link.
- Fix incorrect reference types when retrieving Jaeger traces with multiple
  parent references [#1681].
- Fix incorrect population of span kind field in jaeger getOperations response [#1686]
- Improved query performance when getting trace by id [#1626]

## [0.15.0] - 2022-10-11

### Added
- prom-migrator: Support for passing custom HTTP headers via command line arguments for both
  reader and writer [#1020]
- Run timescaledb-tune with the promscale profile [#1615]
- Propagate the context from received HTTP read requests downstream to database
  requests [#1205]
- Add cmd flag `web.auth.ignore-path` to skip http paths from authentication [#1637]
- Add cmd flag `tracing.streaming-span-writer` to enable/disable streaming span writer. It is enabled by default. [#1633].
- Enable tracing.async-acks by default [#1633].
- Sizes of maintenance worker backlogs exposed as database metrics on the Promscale dashboard [#1634]
- Added a vacuum engine that detects and vacuums/freezes compressed chunks [#1648]
- Add pool of database connections for maintenance jobs e.g. telemetry [#1657]
- Metrics for long-running statements and locks originating from maintenance jobs. [#1661]

### Changed
- Log throughput in the same line for samples, spans and metric metadata [#1643]
- The `chunks_created` metrics was removed. [#1634]
- Stop logging as an error grpc NotFound and Canceled status codes [#1645]
- TimescaleDB is now mandatory [#1660].
- When querying for Jaeger tags with binary values the binary data will be
  returned instead of the base64 representation of the string [#1649].
- Reuse ingestion temp tables across batches [#1679]

### Fixed
- Do not collect telemetry if `timescaledb.telemetry_level=off` [#1612]
- Fix broken cache eviction in clockcache [#1603]
- Possible goroutine leak due to unbuffered channel in select block [#1604]
- Wrap extension upgrades in an explicit transaction [#1665]

## [0.14.0] - 2022-08-30

### Added
- Implement Jaeger gRPC remote storage writer interface [#1543]
- Batching for traces to improve ingest performance along with CLI flags for better control [#1554]
- Helm chart now ships a JSON Schema for imposing a structure of the values.yaml file [#1551]

### Changed
- Helm chart code was migrated to https://github.com/timescale/helm-charts [#1562]
- Deprecate flag `tracing.otlp.server-address` in favour of `tracing.grpc.server-address` [#1588]

### Fixed
- Make Jaeger Event queryable using name and tags [#1553]
- Reset inverted labels cache on epoch change [#1561]
- Error `column "exemplar_label_values" does not exist (SQLSTATE 42703)` on ingesting exemplars [#1574]
- `/labels` & `/label/{name}/values` to respond with labels of authorized tenants only [#1577]

## [0.13.0] - 2022-07-20

### Added
- Network latency metric [#1431]
- Ability to configure reader-pool and writer-pool sizes [#1451]
- Add ability to specify Deployment annotations and Pod annotations in helm charts [#1495]
- Deprecate `openTelemetry` and `prometheus` top-level objects in helm chart in favour of `.service.openTelemetry` and `.service.prometheus` [#1495]
- Allow disabling exposition of promscale port in Service object. [#1495]
- Enable prometheus annotation based scraping only when ServiceMonitor is disabled. [#1495]
- `db.connections.writer-pool.synchronous-commit` controls whether synchronous_commit is enabled/disabled for writer database connections. [#1499]

### Changed
- `db.num-writer-connections` now sets the absolute number of write connections for writing metrics. [#1430]
- Remove flaky PromscaleCacheTooSmall alert  [#1498]

### Fixed
- Refine check for existence of `prom_schema_migrations` table [#1452]
- Do not run rules-manager in `-db.read-only` mode [#1451]
- Fix underlying metric(`promscale_sql_database_chunks_count`) which leads to false positive firing of PromscaleCompressionLow alert [#1494]

## [0.12.1] - 2022-06-29

### Fixed
- Querying traces using boolean tag values [#1457]
- Fix RE2 regex support for label matching [#1441]

## [0.12.0] - 2022-06-21

### Added
- `-enable-feature=promql-per-step-stats` feature for statistics in PromQL evaluation
- Add `readinessProbe` in helm chart [#1266]
- Set number of ingest copiers to the number of DB CPUs [#1387]
- Ability to reload rules and alerting config [#1426]
- Support arrays in trace attribute values [#1381]
- Support for glob in rule_files [#1443]

### Fixed
- Trace query returns empty result when queried with
  - Tags from process table in Jaeger UI [#1385]
  - Tags that have a numeric value, like `http.status_code=200` [#1385]
  - Tags that involve status code [#1384]
- List label values of allowed tenants only [#1427]
- Race condition when stopping ingest [#1370]

## [0.11.0] - 2022-05-11

## Added
- Alerting rules for Promscale. You can find them [here](docs/mixin/alerts/alerts.yaml) [#1181, #1185, #1271]
- Add database status and request metrics [#1185]
- Add database SQL stats as Prometheus metrics. These can be queried under `promscale_sql` namespace [#1193]
- Add alerts for database SQL metrics [#1193]
- Query Jaeger traces directly through Promscale [#1224]
- Additional dataset configuration options via `-startup.dataset.config` flag. Read more [here](docs/dataset.md) [#1276, #1310]
- Support for alerting and recording rules in Promscale [#1286, #1315]
- Support for `/api/v1/rules` & `/api/v1/alerts` API [#1320]
- Log mandatory requirement of Promscale extension when upgrading from older versions [#1329]

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
- Remove deprecated flags. More info can be found [here](docs/configuration.md#old-flag-removal-in-version-0110) [#1229]

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
