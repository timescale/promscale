# Promscale configuration

Promscale accepts configuration via command-line flags, environment variables, or via a `config.yml` file. The basis for environment variable and file-based configuration are the command-line flags.

Should the same configuration parameter be provided by multiple methods, the precedence rules (from highest to lowest) are as follows:
- CLI flag value
- environment variable value
- configuration file value
- default value

## Environment variables

All CLI parameters have an associated environment variable, this can be determined by converting lowercase to uppercase, `-` and `.` to `_`, and prefixing with `PROMSCALE_`. For example, `db.host` can be set via environment variable as `PROMSCALE_DB_HOST`.

## Configuration file

All CLI parameters can be configured with a YAML-formatted configuration file. The file must be a map of key-value pairs, with the keys being CLI flags.

For example, the following config file sets some database parameters.

```yaml
# config.yml
db.ssl-mode: disable
db.user: postgres
db.port: 5432
db.password: password
db.name: postgres
```

If the file is named `config.yml`, Promscale will pick it up automatically, otherwise you can specify the config file with `./promscale -config /path/to/your-config.yml`.

## CLI

The following subsections cover all CLI flags which promscale supports. You can also find the flags for your current promscale binary with `promscale -help`.

### Arguments

| Argument | Description                                                     |
|:--------:|:----------------------------------------------------------------|
| version  | Prints the version information of Promscale.                    |
|   help   | Prints the information related to flags supported by Promscale. |

### General flags

| Flag                            | Type                           | Default               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|---------------------------------|:------------------------------:|:---------------------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cache.memory-target             | unsigned-integer or percentage |          80%          | Target for max amount of memory to use. Specified in bytes or as a percentage of system memory (e.g. 80%).                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| config                          |             string             |      config.yml       | YAML configuration file path for Promscale.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| enable-feature                  |             string             |          ""           | Enable one or more experimental promscale features (as a comma-separated list). Current experimental features are `promql-at-modifier`, `promql-negative-offset` and `promql-per-step-stats`. For more information, please consult the following resources: [promql-at-modifier](https://prometheus.io/docs/prometheus/latest/feature_flags/#modifier-in-promql), [promql-negative-offset](https://prometheus.io/docs/prometheus/latest/feature_flags/#negative-offset-in-promql), [promql-per-step-stats](https://prometheus.io/docs/prometheus/latest/feature_flags/#per-step-stats). |
| thanos.store-api.server-address |             string             |     "" (disabled)     | Address to listen on for Thanos Store API endpoints.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| tracing.otlp.server-address     |             string             |        ":9202"        | GRPC server address to listen on for Jaeger and OTEL traces(DEPRECATED: use `tracing.grpc.server-address` instead).                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| tracing.grpc.server-address     |             string             |        ":9202"        | GRPC server address to listen on for Jaeger and OTEL traces.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| tracing.async-acks              |            boolean             |         true          | Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of traces data in the database. This increases throughput at the cost of a small chance of data loss.                                                                                                                                                                                                                                                                                                                                                                                     |
| tracing.max-batch-size          |            integer             |         5000          | Maximum size of trace batch that is written to DB.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| tracing.batch-timeout           |            duration            |         250ms         | Timeout after new trace batch is created.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| tracing.batch-workers           |            integer             | num of available cpus | Number of workers responsible for creating trace batches. Defaults to number of CPUs.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| tracing.streaming-span-writer   |            boolean             |         true          | Enable/Disable StreamingSpanWriter for grpc based remote jaeger store.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |

### Auth flags

| Flag               | Type   | Default       | Description                                                                          |
|--------------------|:------:|:-------------:|:-------------------------------------------------------------------------------------|
| auth.tls-cert-file | string | "" (disabled) | TLS certificate file path for web server. To disable TLS, leave this field as blank. |
| auth.tls-key-file  | string | "" (disabled) | TLS key file path for web server. To disable TLS, leave this field as blank.         |

### Database flags

| Flag                                          | Type     | Default                        | Description                                                                                                                                                                    |
|-----------------------------------------------|:--------:|:------------------------------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| db.app                                        |  string  |      promscale@{version}       | 'app' sets application_name in database connection string. This is helpful during debugging when looking at pg_stat_activity.                                                  |
| db.connection-timeout                         | duration |           60 seconds           | Timeout for establishing the connection between Promscale and TimescaleDB.                                                                                                     |
| db.connections-max                            | integer  | 80% of possible connections db | Maximum number of connections to the database that should be opened at once. It defaults to 80% of the maximum connections that the database can handle.                       |
| db.connections.num-writers                    | integer  |               0                | Number of database connections for writing metrics to database. By default, this will be set based on the number of CPUs available to the DB Promscale is connected to.        |
| db.connections.reader-pool.size               | integer  | 30% of possible connections db | Maximum size of the reader pool of database connections. This defaults to 30% of max_connections allowed by the database.                                                      |
| db.connections.writer-pool.size               | integer  | 50% of possible connections db | Maximum size of the writer pool of database connections. This defaults to 50% of max_connections allowed by the database.                                                      |
| db.connections.writer-pool.synchronous-commit | boolean  |             false              | Enable/disable synchronous_commit on database connections in the writer pool.                                                                                                  |
| db.host                                       |  string  |           localhost            | Host for TimescaleDB/Vanilla Postgres.                                                                                                                                         |
| db.name                                       |  string  |           timescale            | Database name.                                                                                                                                                                 |
| db.password                                   |  string  |                                | Password for connecting to TimescaleDB/Vanilla Postgres.                                                                                                                       |
| db.port                                       |   int    |              5432              | TimescaleDB/Vanilla Postgres connection password.                                                                                                                              |
| db.read-only                                  | boolean  |             false              | Read-only mode for the connector. Operations related to writing or updating the database are disallowed. It is used when pointing the connector to a TimescaleDB read replica. |
| db.ssl-mode                                   |  string  |            require             | TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass `allow` as value.                                                                        |
| db.statements-cache                           | boolean  |              true              | Whether database connection pool should use cached prepared statements. Disable if using PgBouncer.                                                                            |
| db.uri                                        |  string  |                                | TimescaleDB/Vanilla PostgresSQL URI. Example:`postgres://postgres:password@localhost:5432/timescale?sslmode=require`                                                           |
| db.user                                       |  string  |            postgres            | TimescaleDB/Vanilla Postgres user.                                                                                                                                             |

### Telemetry flags (for telemetry generated by the Promscale connector itself)

| Flag                                     | Type     | Default    | Description                                                                                                                                                                                 |
|------------------------------------------|:--------:|:----------:|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| telemetry.log.format                     |  string  |   logfmt   | Log format to use from [ "logfmt", "json" ].                                                                                                                                                |
| telemetry.log.level                      |  string  |   debug    | Log level to use from [ "error", "warn", "info", "debug" ].                                                                                                                                 |
| telemetry.log.throughput-report-interval | duration |  0 second  | Duration interval at which throughput should be reported. Setting duration to `0` will disable reporting throughput, otherwise, an interval with unit must be provided, e.g. `10s` or `3m`. |
| telemetry.trace.otel-endpoint            |  string  | "" (empty) | OpenTelemetry tracing collector GRPC URL endpoint to send telemetry to. (i.e. otel-collector:4317)                                                                                          |
| telemetry.trace.otel-tls-cert-file       |  string  | "" (empty) | TLS Certificate file used for client authentication against the OTEL tracing collector GRPC endpoint, leave blank to disable TLS.                                                           |
| telemetry.trace.otel-tls-key-file        |  string  | "" (empty) | TLS Key file for client authentication against the OTEL tracing collector GRPC endpoint, leave blank to disable TLS.                                                                        |
| telemetry.trace.jaeger-endpoint          |  string  | "" (empty) | Jaeger tracing collector thrift HTTP URL endpoint to send telemetry to (e.g. https://jaeger-collector:14268/api/traces).                                                                    |
| telemetry.trace.sample-ratio             |  float   |    1.0     | Trace sampling ratio, amount of spans to send to collector. Valid values from 0.0 (none) to 1.0 (all).                                                                                      |

### Metrics specific flags

| Flag                                                | Type                           | Default   | Description                                                                                                                                                                                                                                                                                                                            |
|-----------------------------------------------------|:------------------------------:|:---------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metrics.async-acks                                  |            boolean             |   false   | Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss.                                                                                                                                    |
| metrics.cache.exemplar.size                         |        unsigned-integer        |   10000   | Maximum number of exemplar metrics key-position to cache. It has one-to-one mapping with number of metrics that have exemplar, as key positions are saved per metric basis.                                                                                                                                                            |
| metrics.cache.labels.size                           |        unsigned-integer        |   10000   | Maximum number of labels to cache.                                                                                                                                                                                                                                                                                                     |
| metrics.cache.metrics.size                          |        unsigned-integer        |   10000   | Maximum number of metric names to cache.                                                                                                                                                                                                                                                                                               |
| metrics.cache.series.initial-size                   |        unsigned-integer        |  250000   | Initial number of elements in the series cache.                                                                                                                                                                                                                                                                                        |
| metrics.cache.series.max-bytes                      | unsigned-integer or percentage |    50%    | Target for amount of memory to use for the series cache. Specified in bytes or as a percentage of the memory-target (e.g. 50%).                                                                                                                                                                                                        |
| metrics.high-availability                           |            boolean             |   false   | Enable external_labels based HA.                                                                                                                                                                                                                                                                                                       |
| metrics.ignore-samples-written-to-compressed-chunks |            boolean             |   false   | Ignore/drop samples that are being written to compressed chunks. Setting this to false allows Promscale to ingest older data by decompressing chunks that were earlier compressed. However, setting this to true will save your resources that may be required during decompression.                                                   |
| metrics.multi-tenancy                               |            boolean             |   false   | Use multi-tenancy mode in Promscale.                                                                                                                                                                                                                                                                                                   |
| metrics.multi-tenancy.allow-non-tenants             |            boolean             |   false   | Allow Promscale to ingest/query all tenants as well as non-tenants. By setting this to true, Promscale will ingest data from non multi-tenant Prometheus instances as well. If this is false, only multi-tenants (tenants listed in 'multi-tenancy-valid-tenants') are allowed for ingesting and querying data.                        |
| metrics.multi-tenancy.valid-tenants                 |             string             | allow-all | Sets valid tenants that are allowed to be ingested/queried from Promscale. This can be set as: 'allow-all' (default) or a comma separated tenant names. 'allow-all' makes Promscale ingest or query any tenant from itself. A comma separated list will indicate only those tenants that are authorized for operations from Promscale. |
| metrics.multi-tenancy.experimental.label-queries    |              bool              |   true    | [EXPERIMENTAL] Use label queries that returns labels of authorized tenants only. This may affect system performance while running PromQL queries. By default this is enabled in -metrics.multi-tenancy mode.                                                                                                                           |
| metrics.promql.default-subquery-step-interval       |            duration            | 1 minute  | Default step interval to be used for PromQL subquery evaluation. This value is used if the subquery does not specify the step value explicitly. Example: <metric_name>[30m:]. Note: in Prometheus this setting is set by the evaluation_interval option.                                                                               |
| metrics.promql.lookback-delta                       |            duration            | 5 minute  | The maximum look-back duration for retrieving metrics during expression evaluations and federation.                                                                                                                                                                                                                                    |
| metrics.promql.max-points-per-ts                    |           integer64            |   11000   | Maximum number of points per time-series in a query-range request. This calculation is an estimation, that happens as (start - end)/step where start and end are the 'start' and 'end' timestamps of the query_range.                                                                                                                  |
| metrics.promql.max-samples                          |           integer64            | 50000000  | Maximum number of samples a single query can load into memory. Note that queries will fail if they try to load more samples than this into memory, so this also limits the number of samples a query can return.                                                                                                                       |
| metrics.promql.query-timeout                        |            duration            | 2 minutes | Maximum time a query may take before being aborted. This option sets both the default and maximum value of the 'timeout' parameter in '/api/v1/query.*' endpoints.                                                                                                                                                                     |

### Recording and Alerting rules flags

| Flag                                             | Type     | Default    | Description                                                                                                                                                                                                                                                                                                                                                             |
|--------------------------------------------------|:--------:|:----------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metrics.alertmanager.notification-queue-capacity | integer  |   10000    | The capacity of the queue for pending Alertmanager notifications.                                                                                                                                                                                                                                                                                                       |
| metrics.rules.alert.for-grace-period             | duration | 10 minutes | Minimum duration between alert and restored "for" state. This is maintained only for alerts with configured "for" time greater than grace period.                                                                                                                                                                                                                       |
| metrics.rules.alert.for-outage-tolerance         | duration |   1 hour   | Max time to tolerate Promscale outage for restoring "for" state of alert.                                                                                                                                                                                                                                                                                               |
| metrics.rules.alert.resend-delay                 | duration |  1 minute  | Minimum amount of time to wait before resending an alert to Alertmanager.                                                                                                                                                                                                                                                                                               |
| metrics.rules.config-file                        |  string  |     ""     | Path to configuration file in Prometheus-format, containing rule_files and optional `alerting`, `global` fields. For more details, see https://prometheus.io/docs/prometheus/latest/configuration/configuration/. Note: If this is flag or `rule_files` is empty, Promscale rule-manager will not start. If `alertmanagers` is empty, alerting will not be initialized. |

### Startup process flags

| Flag                                  | Type    | Default       | Description                                                                                                                                                                                                                      |
|---------------------------------------|:-------:|:-------------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| startup.dataset.config                | string  | "" (disabled) | Dataset configuration in YAML format for Promscale. It is used for setting various dataset configuration like default metric chunk interval. For more information, please consult the following resources: [dataset](dataset.md) |
| startup.install-extensions            | boolean |     true      | Install TimescaleDB & Promscale extensions.                                                                                                                                                                                      |
| startup.only                          | boolean |     false     | Only run startup configuration with Promscale (i.e. migrate) and exit. Can be used to run promscale as an init container for HA setups.                                                                                          |
| startup.skip-migrate                  | boolean |     false     | Skip migrating Promscale SQL schema to latest version on startup.                                                                                                                                                                |
| startup.upgrade-extensions            | boolean |     true      | Upgrades TimescaleDB & Promscale extensions.                                                                                                                                                                                     |
| startup.upgrade-prerelease-extensions | boolean |     false     | Upgrades to pre-release TimescaleDB, Promscale extensions.                                                                                                                                                                       |
| startup.use-schema-version-lease      | boolean |     true      | Use schema version lease to prevent race conditions during migration.                                                                                                                                                            |

### Web server flags

| Flag                       | Type    | Default       | Description                                                                                                                                                                                                                 |
|----------------------------|:-------:|:-------------:|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| web.auth.bearer-token      | string  | "" (disabled) | Bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token-file and basic auth methods.                                                                             |
| web.auth.bearer-token-file | string  | "" (disabled) | Path of the file containing the bearer token (JWT) used for web endpoint authentication. Disabled by default. Mutually exclusive with bearer-token and basic auth methods.                                                  |
| web.auth.password          | string  |      ""       | Authentication password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password-file and bearer-token methods.                               |
| web.auth.password-file     | string  |      ""       | Path for auth password file containing the actual password used for web endpoint authentication. This flag should be set together with auth-username. It is mutually exclusive with auth-password and bearer-token methods. |
| web.auth.username          | string  |      ""       | Authentication username used for web endpoint authentication. Disabled by default.                                                                                                                                          |
| web.auth.ignore-path       | string  |      ""       | HTTP paths which has to be skipped from authentication. This flag shall be repeated and each one would be appended to the ignore list.                                                                                      |
| web.cors-origin            | string  |     `.*`      | Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1                                                                                                                                                    |
| web.enable-admin-api       | boolean |     false     | Allow operations via API that are for advanced users. Currently, these operations are limited to deletion of series.                                                                                                        |
| web.listen-address         | string  |    `:9201`    | Address to listen on for web endpoints.                                                                                                                                                                                     |
| web.telemetry-path         | string  |  `/metrics`   | Web endpoint for exposing Promscale's Prometheus metrics.                                                                                                                                                                   |

## Old flag removal in version 0.11.0

With version 0.11.0, we are removing old versions of flag names and enviromental variables. If you run Promscale with those old names, you should get a warning with a suggestion to update the name to the corresponding flag name or environmental variable.

This is the list of CLI flags that are removed in favor of the new flag names:

-app
-async-acks
-auth-password
-auth-password-file
-auth-username
-bearer-token
-bearer-token-file
-db-connection-timeout
-db-connections-max
-db-host
-db-name
-db-password
-db-port
-db-ssl-mode
-db-statements-cache
-db-uri
-db-user
-db-writer-connection-concurrency
-exemplar-cache-size
-high-availability
-ignore-samples-written-to-compressed-chunks
-install-extensions
-labels-cache-size
-log-format
-log-level
-memory-target
-metrics-cache-size
-migrate
-multi-tenancy
-multi-tenancy-allow-non-tenants
-multi-tenancy-valid-tenants
-otlp-grpc-server-listen-address
-promql-default-subquery-step-interval
-promql-lookback-delta
-promql-max-points-per-ts
-promql-max-samples
-promql-query-timeout
-read-only
-series-cache-initial-size
-series-cache-max-bytes
-thanos-store-api-listen-address
-tls-cert-file
-tls-key-file
-tput-report
-upgrade-extensions
-upgrade-prerelease-extensions
-use-schema-version-lease
-web-cors-origin
-web-enable-admin-api
-web-listen-address
-web-telemetry-path

One special case here is the `migrate` flag which has no direct translation to the new name. Instead, we suggest using `-startup.skip-migrate` or `-startup.only` flags depending on the value used with the old flag.
