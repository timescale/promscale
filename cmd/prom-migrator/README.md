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
3. Prometheus tsdb (read, TODO in next version)
4. Thanos (read, write)
5. Cortex (only blocks storage, chunks storage in later versions) (read, write)
6. VictoriaMetrics (read, write, not sure about backfill)
7. M3DB (read, write, not sure about backfill)
8. Others (any remote storage systems with remote read (for reading) and write (for writing) endpoints)

It should be noted that some of the above systems do not support backfill and so their ability to serve
as write endpoints are somewhat constrained.

#### Example

```
./prom-migrator -mint=1606408552 -maxt=1606415752 -read-url=<read_endpoint_url_for_remote_read_storage> -write-url=<write_endpoint_url_for_remote_write_storage> -progress-metric-url=<read_endpoint_url_for_remote_write_storage>
```

## CLI flags

### General flags

| Flag | Type | Required | Default | Description |
|:------:|:-----:|:-------:|:------:|:-----------|
| mint | int64 | true | 0 | Minimum timestamp (in seconds unix) for carrying out data migration. (inclusive) |
| read-url | string | true | | URL address for the storage where the data is to be read from. |
| write-url | string | true | | URL address for the storage where the data migration is to be written. |
| maxt | int64 | false | time.Now().Unix() | Maximum timestamp (in seconds unix) for carrying out data migration (exclusive). Setting this value less than zero will indicate all data from mint upto now. |
| migration-name | string | false | prom-migrator | Name for the current migration that is to be carried out. It corresponds to the value of the label 'job' set inside the progress-metric-name.|
| max-read-size | string | false | 500MB | (units: B, KB, MB, GB, TB, PB) the maximum size of data that should be read at a single time. More the read size, faster will be the migration but higher will be the memory usage. Example: 250MB. |
| progress-enabled | boolean | false | true | This flag tells the migrator, whether or not to use the progress mechanism. It is helpful if you want to carry out migration with the same time-range. If this is enabled, the migrator will resume the migration from the last time, where it was stopped/interrupted. If you do not want any extra metric(s) while migration, you can set this to false. But, setting this to false will disble progress-metric and hence, the ability to resume migration. |
| progress-metric-name | string | false | prom_migrator_progress | Prometheus metric name for tracking thelast maximum timestamp pushed to the remote-write storage. This is used to resume the migration process after a failure. |
| progress-metric-url | string | false | | URL of the remote storage that contains the progress-metric. Note: This url is used to fetch the last pushed timestamp. If you want the migration to resume from where it left, in case of a crash, set this to the remote write storage that the migrator is writing along with the progress-enabled. |
| concurrent-pulls | int | false | 1 | Concurrent pulls enables fetching of data concurrently. This may enable higher throughput by pulling data faster from remote-read stores. Note: Setting concurrent-pulls > 1 will show progress of concurrent fetching of data in the progress-bar and disable real-time transfer rate speed while fetching data. Moreover, setting this value too high may cause TLS handshake error on the read storage side or may lead to starvation of fetch requests, depending on the bandwidth. |
| concurrent-push | int | false | 1 | Concurrent push enables pushing of slabs concurrently. Each slab is divided into 'concurrent-push' (value) parts and then pushed to the remote-write storage concurrently. This may lead to higher throughput on the remote-write storage provided it is capable of handling the load. Note: Larger shards count will lead to significant memory usage. |

**Note:** A simple way to find timestamp in seconds unix, you can simply query in prometheus's (or related
platform) UI and see the start/end timestamp. If in decimal, the part prior to the decimal point will be
the timestamp in seconds unix.

### Authentication flags

#### Read storage
| Flag | Type | Description |
|:------:|:-----:|:------------------|
| read-auth-username | string | Auth username for remote-read storage. |
| read-auth-password | string | Auth password for remote-read storage. |
| read-auth-bearer-token | string | Bearer token for remote-read storage. This should be mutually exclusive with username and password. |

#### Write storage
| Flag | Type | Description |
|:------:|:-----:|:------------------|
| write-auth-username | string | Auth username for remote-write storage. |
| write-auth-password | string | Auth password for remote-write storage. |
| write-auth-bearer-token | string | Bearer token for remote-write storage. This should be mutually exclusive with username and password. |

**Note:** Currently, credentials are expected to be supplied via CLI. In later versions,
reading credentials from files (recommended for security reasons than providing credentials through CLI)
will be supported.

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
