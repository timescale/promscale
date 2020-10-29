# Promscale command line doc

This document gives you information about the configuration flags supported by promscale.
You can also find information on flags with `promscale_<version> -help` 

## General flags
| Flag | Type | Default | Description |
|------|:-----:|:-------:|:-----------:|
| install-timescaledb | boolean | true | Install or update TimescaleDB extension. |
| leader-election-pg-advisory-lock-id | integer | 0 (disabled) | Leader-election based high-availability. It is based on PostgreSQL advisory lock and requires a unique advisory lock ID per high-availability group. Only a single connector in each high-availability group will write data at one time. A value of 0 disables leader election. |
| leader-election-pg-advisory-lock-prometheus-timeout | slack/duration | -1 | Prometheus timeout duration for leader-election high-availability. The connector will resign if the associated Prometheus instance does not send any data within the given timeout. This value should be a low multiple of the Prometheus scrape interval, big enough to prevent random flips. |
| leader-election-scheduled-interval | duration | 5 seconds | Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock. |
| log-format | string | logfmt | Log format to use from [ "logfmt", "json" ]. |
| log-level | string | debug | Log level to use from [ "error", "warn", "info", "debug" ]. |
| labels-cache-size | unsigned-integer | 10000 | Maximum number of labels to cache. |
| metrics-cache-size | unsigned-integer | 10000 | Maximum number of metric names to cache. |
| migrate | string | true | Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only]. |
| read-only | boolean | false | Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica. |
| use-schema-version-lease | boolean | true | Use schema version lease to prevent race conditions during migration. |
| tput-report | integer | 0 (disabled) | Interval in seconds at which throughput should be reported. |
| web-cors-origin | string | `.*` |  Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com' |
| web-enable-admin-api | boolean | false | Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series. |
| web-listen-address | string | `:9201` | Address to listen on for web endpoints. |
| web-telemetry-path | string | `/metrics` | Web endpoint for exposing Promscale's Prometheus metrics. |

## Database flags

| Flag | Type | Default | Description |
|------|:-----:|:-------:|:-----------:|
| db-host | string | localhost | Host for TimescaleDB/Vanilla Postgres. |
| db-port | int | 5432 | TimescaleDB/Vanilla Postgres connection password. |
| db-name | string | timescale | Database name. |
| db-password | string | | Password for connecting to TimescaleDB/Vanilla Postgres. |
| db-user | string | postgres | TimescaleDB/Vanilla Postgres user. |
| db-connect-retries | integer | 0 | Number of retries Promscale should make for establishing connection with the database. |
| db-connections-max | integer | 80% of possible connections db | Maximum number of connections to the database that should be opened at once. It defaults to 80% of the maximum connections that the database can handle. |
| db-ssl-mode | string | require | TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass `allow` as value. |
| db-writer-connection-concurrency | int | 4 | Maximum number of database connections for writing per go process. |
| async-acks | boolean | false | Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss. |
