# Dataset configuration format

Promscale stores some configuration information in the Postgres database which it is connected to. We call this configuration the _Promscale dataset configuration_. Promscale accepts an option to set the dataset values. This document describes its format and mechanics.

Setup is done by using the `-startup.dataset.config` flag which should contain this configuration structure in YAML format

Example usage:
```bash
promscale -startup.dataset.config=$'metrics:\n  default_chunk_interval: 6h'
```

The expected format is YAML.

The YAML values are easier to set when using `config.yaml` instead of the cli flags. This would look as follows:

```yaml
startup.dataset.config: |
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

| Section | Setting                  |   Type   | Default | Description                                                                                                     |
|:--------|:-------------------------|:--------:|:-------:|:----------------------------------------------------------------------------------------------------------------|
| metric  | default_chunk_interval   | duration |   8h    | Chunk interval used to create hypertable chunks that store the metric data                                      |
| metric  | compress_data            |   bool   |  true   | Boolean setting to turn on or off compression of metric data                                                    |
| metric  | ha_lease_refresh         | duration |   10s   | High availability lease refresh duration, period after which the lease will be refreshed                        |
| metric  | ha_lease_timeout         | duration |   1m    | High availability lease timeout duration, period after which the lease will be lost in case it wasn't refreshed |
| metric  | default_retention_period | duration |   90d   | Retention period for metric data, all data older than this period will be dropped                               |
| traces  | default_retention_period | duration |   90d   | Retention period for tracing data, all data older than this period will be dropped                              |
