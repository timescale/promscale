# Dataset configuration format

This document describes the format of the configuration structure used to setup the Promscale dataset options. Setup is done by using the `-startup.dataset.config` flag which should contain this configuration structure in YAML format

Example usage:
```
promscale -startup.dataset.config=$'metrics:\n  default_chunk_interval: 6h'
```

Expected format is YAML and currently only option that can be set is default chunk interval.

Example configuration in config.yaml:

```
startup.dataset.config: |
  metrics:
    default_chunk_interval: 6h
```

Above configuration will set the default chunk interval to 6 hours.

Note: Any configuration omitted from the configuration structure will be set to its default value.


## Default values

| Setting | Type | Default | Description |
|:--------|:----:|:-------:|:------------|
| default_chunk_interval | duration | 8h | Chunk interval used to create hypertable chunks that store the metric data |
