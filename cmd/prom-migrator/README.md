# Prom-migrator

Prom-migrator is an **open-source**, **community-driven** and **free-to-use**, **universal prometheus
data migration tool**, that migrates data from one storage system to another, leveraging Prometheus's
remote storage endpoints. You can read more about Prom-migrator in our [blog post](https://blog.timescale.com/blog/introducing-prom-migrator-a-universal-open-source-prometheus-data-migration-tool/).


Click the video below for an overview of Prom-migrator:

<p align="center">
<a href="https://www.youtube.com/watch?v=8h6tSDKdlbA"> <img src="https://media.giphy.com/media/wbt3Cnw5jOtUZjxZqI/giphy.gif"> </a>
</p>

### Unique features

* Migrate prometheus data from one storage system to another
* Pretty outputs during runtime, allowing users to track progress
* Ability to resume migration(s) from last sent data, in case of any unintended shutdowns
* Ability to attempt for retry in case of timeout or error
* Stateless working model

## Overview

This document contains:
1. [**Working overview**](#working-overview), that describes in brief, the working model of prom-migrator
2. [**Storage systems**](#storage-systems)
3. [**CLI flags**](#cli-flags)
4. [**Contributing**](#contributing)
5. [**Help**](#help)

## Working overview

Prom-migrator migrates the data from one storage to another. It pulls the data from a particular remote
storage system using the remote-read endpoint for a certain time-range. It then pushes the data to another
remote storage system using the remote-write endpoint. It continues by reading and then writing more data
until it finishes the entire time-range specified by the user. The time-range of any individual read is
adaptively auto-adjusted to bound the overall memory usage of the system.

The system is able to auto-resume the migration if a previous migration was interrupted. It does this by
adding a progress-metric to the data as it is writing. The sample of the progress metric records the
maximum time that was written. Thus, when a migration process resumes, it simply reads the progress-metric
to find out what was last written and picks up where it left off. 

For detailed information about working, design and process,
refer to [https://tsdb.co/prom-migrator-design-doc](https://tsdb.co/prom-migrator-design-doc)

## Storage systems

Storage systems that we aim to support with prom-migrator are:

Key:
* read - means data can be read from system,
* write - means data can be written to an empty database,
* backfill - means data can be written to a database that already contains data newer than what is being
  inserted.

1. Promscale (read, write, backfill)
2. Prometheus (read)
3. Prometheus tsdb (TODO)
4. Thanos (read, write)
5. Cortex (only blocks storage, chunks storage in later versions) (read, write)
6. VictoriaMetrics (read, write, not sure about backfill)
7. M3DB (read, write, not sure about backfill)
8. Others (any remote storage systems with remote read (for reading) and write (for writing) endpoints)

It should be noted that some above systems do not support backfill and so their ability to serve
as write endpoints are somewhat constrained.

#### Example

```shell
./prom-migrator -start=1606408552 -end=1606415752 -reader-url=<read_endpoint_url_for_remote_read_storage> -writer-url=<write_endpoint_url_for_remote_write_storage> -progress-metric-url=<read_endpoint_url_for_remote_write_storage>
```

## CLI flags

### General flags

| Flag | Type | Required | Default | Description |
|:------:|:-----:|:-------:|:------:|:-----------|
| start | string | true | `"1970-01-01T00:00:00+00:00"` | Start time (in RFC3339 format, like '1970-01-01T00:00:00+00:00', or in number of seconds since the unix epoch, UTC) from which the data migration is to be carried out. (inclusive) |
| end | string | false | `""` (corresponds to `time.Now()`) | End time (in RFC3339 format, like '1970-01-01T00:00:00+00:00', or in number of seconds since the unix epoch, UTC) for carrying out data migration (exclusive). By default if this value is unset i.e., `""`, then 'end' will be set to the time at which migration is starting. |
| reader-url | string | true | `""` | URL address for the storage where the data is to be read from. |
| reader-max-retries | int | false | `0` | Maximum number of retries before erring out. Setting this to 0 will make the retry process forever until the process is completed. Note: If you want not to retry, change the value of on-timeout or on-error to non-retry options. |
| reader-on-error | string | false | `"abort"` | When an error occurs during read process, how should the reader behave. Valid options: ['retry', 'skip', 'abort']. See 'reader-on-timeout' for more information on the above options. |
| reader-on-timeout | string | false | `"retry"` | When a timeout happens during the read process, how should the reader behave. Valid options: ['retry', 'skip', 'abort']. If 'retry', the reader retries to fetch the current slab after the delay. If 'skip', the reader skips the current slab that is being read and moves on to the next slab. If 'abort', the migration process will be aborted. |
| reader-retry-delay | duration | false | `1 second` | Duration to wait after a 'read-timeout' before prom-migrator retries to fetch the slab. Delay is used only if 'retry' option is set in OnTimeout or OnErr. |
| reader-metrics-matcher | string | false | `"{__name__=~".*"}"` | Metrics vector selector to read data for migration. |
| writer-url | string | true | `""` | URL address for the storage where the data migration is to be written. |
| writer-on-error | string | false | `"abort"` | When an error occurs during write process, how should the writer behave. Valid options: ['retry', 'skip', 'abort']. See 'writer-on-timeout' for more information on the above options. |
| writer-on-timeout | string | false | `"retry"` | When a timeout happens during the write process, how should the writer behave. Valid options: ['retry', 'skip', 'abort']. If 'retry', the writer retries to push the current slab after the delay. If 'skip', the writer skips the current slab that is being pushed and moves on to the next slab. If 'abort', the migration process will be aborted. |
| writer-retry-delay | duration | false | `1 second` | Duration to wait after a 'write-timeout' before prom-migrator retries to push the slab. Delay is used only if 'retry' option is set in OnTimeout or OnErr. |
| writer-timeout | duration | false | `5 minutes` | Timeout for pushing data to write storage. |
| concurrent-pull | integer | false | `1` | Concurrent pull enables fetching of data concurrently. Each fetch query is divided into 'concurrent-pull' (value) parts and then fetched concurrently. This allows higher throughput of read by pulling data faster from the remote-read storage. Note: Setting 'concurrent-pull' > 1 will show progress of concurrent fetching of data in the progress-bar and disable real-time transfer rate. High 'concurrent-pull' can consume significant memory, so make sure you balance this with your number of migrating series and available memory. Also, setting this value too high may cause TLS handshake error on the read storage side or may lead to starvation of fetch requests, depending on your network bandwidth. |
| concurrent-push | integer | false | `1` | Concurrent push enables pushing of slabs concurrently. Each slab is divided into 'concurrent-push' (value) parts and then pushed to the remote-write storage concurrently. This may lead to higher throughput on the remote-write storage provided it is capable of handling the load. Note: Larger shards count will lead to significant memory usage. |
| gc-on-push | boolean | false | `false` | Run garbage collector after every slab is pushed. This may lead to better memory management since GC is kick right after each slab to clean unused memory blocks. |
| slab-range-increment | duration | false | `1 minute` | Amount of time-range to be incremented in successive slab. |
| max-read-duration | duration | false | `2h` | Maximum range of slab that can be achieved through consecutive 'la-increment'. This defaults to '2 hours' since assuming the migration to be from Prometheus TSDB. Increase this duration if you are not fetching from Prometheus or if you are fine with slow read on Prometheus side post 2 hours slab time-range. |
| max-read-size | string | false | `500MB` | (units: B, KB, MB, GB, TB, PB) the maximum size of data that should be read at a single time. More the read size, faster will be the migration but higher will be the memory usage. Example: 250MB. |
| migration-name | string | false | `prom-migrator` | Name for the current migration that is to be carried out. It corresponds to the value of the label 'job' set inside the progress-metric-name. |
| progress-enabled | boolean | false | `true` | This flag tells the migrator, whether or not to use the progress mechanism. It is helpful if you want to carry out migration with the same time-range. If this is enabled, the migrator will resume the migration from the last time, where it was stopped/interrupted. If you do not want any extra metric(s) while migration, you can set this to false. But, setting this to false will disable progress-metric and hence, the ability to resume migration. |
| progress-metric-name | string | false | `prom_migrator_progress` | Prometheus metric name for tracking the last maximum timestamp pushed to the remote-write storage. This is used to resume the migration process after a failure. |
| progress-metric-url | string | false | `""` | URL of the remote storage that contains the progress-metric. Note: This url is used to fetch the last pushed timestamp. If you want the migration to resume from where it left, in case of a crash, set this to the remote write storage that the migrator is writing along with the progress-enabled. |

**Note:** A simple way to find timestamp in seconds unix, you can simply query in Prometheus's (or related
platform) UI and see the start/end timestamp. If in decimal, the part prior to the decimal point will be
the timestamp in seconds unix.

### Authentication flags

**Note:** Currently, credentials are expected to be supplied via CLI. In later versions,
reading credentials from files (recommended for security reasons than providing credentials through CLI)
will be supported.

#### Progress-metric

| Flag | Type | Required | Default | Description |
|:------:|:-----:|:-------:|:------:|:-----------|
| progress-metric-auth-username | string | false | `""` | Read auth username for remote-write storage. |
| progress-metric-bearer-token | string | false | `""` | Read bearer-token for remote-write storage. This should be mutually exclusive with username and password and bearer-token file. |
| progress-metric-bearer-token-file | string | false | `""` | Read bearer-token for remote-write storage. This should be mutually exclusive with username and password and bearer-token. |
| progress-metric-password | string | false | `""` | Read auth password for remote-write storage. Mutually exclusive with password-file. |
| progress-metric-password-file | string | false | `""` | Read auth password for remote-write storage. Mutually exclusive with password. |
| progress-metric-tls-ca-file | string | false | `""` | TLS CA file for progress-metric component. |
| progress-metric-tls-cert-file | string | false | `""` | TLS certificate file for progress-metric component. |
| progress-metric-tls-insecure-skip-verify | boolean | false | `false` |  TLS insecure skip verify for progress-metric component. |
| progress-metric-tls-key-file | string | false | `""` | TLS key file for progress-metric component. |
| progress-metric-tls-server-name | string | false | `""` | TLS server name for progress-metric component. |

#### Reader

| Flag | Type | Required | Default | Description |
|:------:|:-----:|:-------:|:------:|:-----------|
| reader-auth-bearer-token | string | false | `""` | Bearer-token for remote-read storage. This should be mutually exclusive with username and password and bearer-token file. |
| reader-auth-bearer-token-file | string | false | `""` |  Bearer-token file for remote-read storage. This should be mutually exclusive with username and password and bearer-token. |
| reader-auth-password | string | false | `""` | Auth password for remote-read storage. Mutually exclusive with password-file. |
| reader-auth-password-file | string | false | `""` | Auth password-file for remote-read storage. Mutually exclusive with password. |
| reader-auth-username | string | false | `""` | Auth username for remote-read storage. |
| reader-timeout | duration | false | `5 minutes` | Timeout for fetching data from read storage. This timeout is also used to fetch the progress metric. |
| reader-tls-ca-file | string | false | `""` | TLS CA file for remote-read component. |
| reader-tls-cert-file | string | false | `""` | TLS certificate file for remote-read component. |
| reader-tls-insecure-skip-verify | boolean | false | `false` | TLS insecure skip verify for remote-read component. |
| reader-tls-key-file | string | false | `""` | TLS key file for remote-read component. |
| reader-tls-server-name | string | false | `""` | TLS server name for remote-read component. |

#### Writer

| Flag | Type | Required | Default | Description |
|:------:|:-----:|:-------:|:------:|:-----------|
| writer-auth-bearer-token | string | false | `""` | Bearer-token for remote-write storage. This should be mutually exclusive with username and password and bearer-token file. |
| writer-auth-bearer-token-file | string | false | `""` | Bearer-token for remote-write storage. This should be mutually exclusive with username and password and bearer-token. |
| writer-auth-password | string | false | `""` | Auth password for remote-write storage. Mutually exclusive with password-file. |
| writer-auth-password-file | string | false | `""` | Auth password-file for remote-write storage. Mutually exclusive with password. |
| writer-auth-username | string | false | `""` | Auth username for remote-write storage. |
| writer-max-retries | integer | false | `0` | Maximum number of retries before erring out. Setting this to 0 will make the retry process forever until the process is completed. Note: If you want not to retry, change the value of on-timeout or on-error to non-retry options. |
| writer-tls-ca-file | string | false | `""` | TLS CA file for remote-writer component. |
| writer-tls-cert-file | string | false | `""` | TLS certificate file for remote-writer component. |
| writer-tls-insecure-skip-verify | boolean | false | `false` | TLS insecure skip verify for remote-writer component. |
| writer-tls-key-file | string | false | `""` | TLS key file for remote-writer component. |
| writer-tls-server-name | string | false | `""` | TLS server name for remote-writer component. |

## Contributing

We welcome contributions to the Promscale Connector, which is
licensed and released under the open-source Apache License, Version 2.
The same [Contributor's
Agreement](//github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
applies as in TimescaleDB; please sign the [Contributor License
Agreement](https://cla-assistant.io/timescale/promscale)
(CLA) if you're a new contributor.

In case of any issues/bugs/support, please feel free to open github issues at the
[promscale repo issues page](https://github.com/timescale/promscale/issues).

## Help

For additional details/queries/feedback/support, please feel free to ask us on the
**#promscale** channel on [TimescaleDB Slack](https://slack.timescale.com/).
You can also email us at **[promscale@timescale.com](emailto:promscale@timescale.com)**
or **meet us virtually** by joining the promscale community call, which is held on second
wednesday of every month on [zoom (link)](https://zoom.us/j/99530717170?pwd=N1hDOU9qNSt1SW9iaDZiUDBQWG9aQT09)
([agenda link](https://tsdb.co/promscale-agenda)) or open topics on the required help at
[Promscale Users Google Group](https://groups.google.com/forum/#!forum/promscale-users).
