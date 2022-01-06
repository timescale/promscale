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
- Add ability to configure chunk interval on startup

### Changed
- Rename CLI flags to improve user interface

### Deprecated
- Deprecate `migrate` flag

### Removed
- Remove deprecated `-promql-enable-feature` flag
- Remove deprecated leader election

### Fixed
- helm-charts: use fixed target port on svc-promscale
- Fix passing of async flag
- Remove the event_name_check constraint
- Fix _prom_catalog.metric_view() function on non-tsdb installs

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