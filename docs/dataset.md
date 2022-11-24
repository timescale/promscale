# Dataset configuration format

Promscale stores some configuration information in the Postgres database which it is connected to. We call this configuration the *Promscale dataset configuration*. Promscale accepts an option to set the dataset values. This document describes its format and mechanics.

Setup is done via the `config.yaml` under `startup.dataset`:

```yaml
startup:
  dataset:
    metrics:
      default_chunk_interval: 6h
      compress_data: true
      ha_lease_refresh: 10s
      ha_lease_timeout: 1m
      default_retention_period: 90d
    traces:
      default_retention_period: 30d
```

Note: Any configuration omitted from the configuration structure will be set to its default value.

## Configurable values and their defaults

| Section | Setting                  | Type     | Default | Description                                                                                                     |
|:--------|:-------------------------|:--------:|:-------:|:----------------------------------------------------------------------------------------------------------------|
| metrics | default_chunk_interval   | duration |   8h    | Chunk interval used to create hypertable chunks that store the metric data                                      |
| metrics | compress_data            |   bool   |  true   | Boolean setting to turn on or off compression of metric data                                                    |
| metrics | ha_lease_refresh         | duration |   10s   | High availability lease refresh duration, period after which the lease will be refreshed                        |
| metrics | ha_lease_timeout         | duration |   1m    | High availability lease timeout duration, period after which the lease will be lost in case it wasn't refreshed |
| metrics | default_retention_period | duration |   90d   | Retention period for metric data, all data older than this period will be dropped                               |
| traces  | default_retention_period | duration |   90d   | Retention period for tracing data, all data older than this period will be dropped                              |

## Upgrading from startup.dataset.config

The flag `startup.dataset.config` accepts the string representation of YAML.
This was so that it could be used not only in the config file, but also as
a command line flag or environment variable.

In a config file:

```yaml
startup.dataset.config: |
  metrics:
    default_chunk_interval: 8h
```

Or as a command line flag:

```bash
./promscale -startup.dataset.config="metrics:\n  default_chunk_interval: 8h"
```

Or as an environment variable

```bash
PROMSCALE_STARTUP_DATASET_CONFIG="metrics:\n  default_chunk_interval: 8h" ./promscale
```

Using a string has some implications, specially for validation in a
Kubernetes environment. In a future release of Promscale,
`statup.dataset.config` will be deprecated in favor of `startup.dataset`
which is a YAML mapping node, instead of a string, that can only be set
using the config file:

Old to be deprecated Config:

```yaml
startup.dataset.config: |
  metrics:
    default_chunk_interval: 8h
```

Recommended dataset config as YAML mapping

```
startup:
  dataset:
    metrics:
      default_chunk_interval: 8h
```
