# Promscale command line doc

This document gives you information about the configuration flags and arguments supported by Promscale.
You can also find information on flags with `promscale_<version> -help`.

**Note:** Flags can be set as environment variables by converting `-` to `_` (if any), and prefixing with `PROMSCALE_`. For example, `db-host` can be set as an environment variable as `PROMSCALE_DB_HOST`.

## Arguments
| Argument | Description |
|:------:|:-----|
| version | Prints the version information of Promscale. |
| help | Prints the information related to flags supported by Promscale.

## General flags
| Flag | Type | Default | Description |
|:------:|:-----:|:-------:|:-----------|
| config | string | config.yml | YAML configuration file path for Promscale. |
| install-extensions | boolean | true | Install TimescaleDB & Promscale extensions. |
| upgrade-extensions | boolean | true | Upgrades TimescaleDB & Promscale extensions. |
| upgrade-prerelease-extensions | boolean | false | Upgrades to pre-release TimescaleDB, Promscale extensions. |
| leader-election-pg-advisory-lock-id | integer | 0 (disabled) | Leader-election based high-availability. It is based on PostgreSQL advisory lock and requires a unique advisory lock ID per high-availability group. Only a single connector in each high-availability group will write data at one time. A value of 0 disables leader election. |
| leader-election-pg-advisory-lock-prometheus-timeout | slack/duration | -1 | Prometheus timeout duration for leader-election high-availability. The connector will resign if the associated Prometheus instance does not send any data within the given timeout. This value should be a low multiple of the Prometheus scrape interval, big enough to prevent random flips. |
| leader-election-scheduled-interval | duration | 5 seconds | Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock. |
| log-format | string | logfmt | Log format to use from [ "logfmt", "json" ]. |
| log-level | string | debug | Log level to use from [ "error", "warn", "info", "debug" ]. |
| migrate | string | true | Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only]. |
| read-only | boolean | false | Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica. |
| use-schema-version-lease | boolean | true | Use schema version lease to prevent race conditions during migration. |
| async-acks | boolean | false | Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss. |
| tput-report | duration | 1 second | Setting duration to `0` will disable reporting throughput, else interval with unit at which throughput should be reported, e.g. `10s` or `3m`. |
| tls-cert-file | string | "" (disabled) | TLS certificate file path for web server. To disable TLS, leave this field as blank. |
| tls-key-file | string | "" (disabled) | TLS key file path for web server. To disable TLS, leave this field as blank. |
| web-cors-origin | string | `.*` |  Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com' |
| web-enable-admin-api | boolean | false | Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series. |
| web-listen-address | string | `:9201` | Address to listen on for web endpoints. |
| web-telemetry-path | string | `/metrics` | Web endpoint for exposing Promscale's Prometheus metrics. |

## Resource usage flags
| Flag | Type | Default | Description |
|------|:-----:|:-------:|:-----------|
| memory-target | unsigned-integer or percentage | 80% | Target for max amount of memory to use. Specified in bytes or as a percentage of system memory (e.g. 80%). |
| labels-cache-size P| unsigned-integer | 10000 | Maximum number of labels to cache. |
| metrics-cache-size | unsigned-integer | 10000 | Maximum number of metric names to cache. |
| series-cache-initial-size | unsigned-integer| 250000 | Initial number of elements in the series cache. |
| series-cache-max-bytes | unsigned-integer or percentage | 50% |  Target for amount of memory to use for the series cache. Specified in bytes or as a percentage of the memory-target (e.g. 50%). |

## Auth flags

| Flag | Type | Default | Description |
|:------:|:-----:|:-------:|:-----------|
| auth-username | string | "" | Authentication username used for web endpoint authentication. Disabled by default. |
| auth-password | string | "" | Authentication password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password-file and bearer-token methods. |
| auth-password-file | string | "" | Path for auth password file containing the actual password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password and bearer-token methods. |
| bearer-token | string | "" (disabled) | Bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token-file and basic auth methods. |
| bearer-token-file | string | "" (disabled) | Path of the file containing the bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token and basic auth methods. |

## Multi-tenancy flags

| Flag | Type | Default | Description |
|:------:|:-----:|:-------:|:-----------|
| multi-tenancy | boolean | false | Use multi-tenancy mode in Promscale. |
| multi-tenancy-allow-non-tenants | boolean | false | Allow Promscale to ingest/query all tenants as well as non-tenants. By setting this to true, Promscale will ingest data from non multi-tenant Prometheus instances as well. If this is false, only multi-tenants (tenants listed in 'multi-tenancy-valid-tenants') are allowed for ingesting and querying data. |
| multi-tenancy-valid-tenants | string | allow-all |  Sets valid tenants that are allowed to be ingested/queried from Promscale. This can be set as: 'allow-all' (default) or a comma separated tenant names. 'allow-all' makes Promscale ingest or query any tenant from itself. A comma separated list will indicate only those tenants that are authorized for operations from Promscale. |

## Database flags

| Flag | Type | Default | Description |
|:------:|:-----:|:-------:|:-----------|
| app | string | promscale@{version} | 'app' sets application_name in database connection string. This is helpful during debugging when looking at pg_stat_activity. |
| db-host | string | localhost | Host for TimescaleDB/Vanilla Postgres. |
| db-port | int | 5432 | TimescaleDB/Vanilla Postgres connection password. |
| db-name | string | timescale | Database name. |
| db-password | string | | Password for connecting to TimescaleDB/Vanilla Postgres. |
| db-user | string | postgres | TimescaleDB/Vanilla Postgres user. |
| db-connect-retries | integer | 0 | Number of retries Promscale should make for establishing connection with the database. |
| db-connection-timeout | duration | 60 seconds | Timeout for establishing the connection between Promscale and TimescaleDB. |
| db-connections-max | integer | 80% of possible connections db | Maximum number of connections to the database that should be opened at once. It defaults to 80% of the maximum connections that the database can handle. |
| db-ssl-mode | string | require | TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass `allow` as value. |
| db-writer-connection-concurrency | integer | 4 | Maximum number of database connections for writing per go process. |
| db-uri | string | | TimescaleDB/Vanilla PostgresSQL URI. Example:`postgres://postgres:password@localhost:5432/timescale?sslmode=require` |
| db-statements-cache | boolean | true | Whether database connection pool should use cached prepared statements. Disable if using PgBouncer. |
| ignore-samples-written-to-compressed-chunks | boolean | false | Ignore/drop samples that are being written to compressed chunks. Setting this to false allows Promscale to ingest older data by decompressing chunks that were earlier compressed. However, setting this to true will save your resources that may be required during decompression. |
| async-acks | boolean | false | Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss. |

## PromQL engine evaluation flags

| Flag | Type | Default | Description |
|:------:|:-----:|:-------:|:-----------|
| promql-enable-feature | string | "" | [EXPERIMENTAL] Enable optional PromQL features, separated by commas. These are disabled by default in Promscale's PromQL engine. Currently, this includes 'promql-at-modifier' and 'promql-negative-offset'. For more information, see https://github.com/prometheus/prometheus/blob/master/docs/disabled_features.md |
| promql-query-timeout | duration | 2 minutes | Maximum time a query may take before being aborted. This option sets both the default and maximum value of the 'timeout' parameter in '/api/v1/query.*' endpoints. |
| promql-default-subquery-step-interval | duration | 1 minute | Default step interval to be used for PromQL subquery evaluation. This value is used if the subquery does not specify the step value explicitly. Example: <metric_name>[30m:]. Note: in Prometheus this setting is set by the evaluation_interval option. |
| promql-lookback-delta | duration | 5 minute | The maximum look-back duration for retrieving metrics during expression evaluations and federation. |
| promql-max-samples | integer64 | 50000000 | Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return. |
| promql-max-points-per-ts  | integer64 | 11000 | Maximum number of points per time-series in a query-range request. This calculation is an estimation, that happens as (start - end)/step where start and end are the 'start' and 'end' timestamps of the query_range. |
