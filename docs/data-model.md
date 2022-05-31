# Promscale Data Storage

This document provides the foundations to understand how Promscale stores data.

## Metrics

The most important components of the metric data model are as follows:

```
                            _prom_catalog
      ┌───────────────────────────────────────────────────────┐
      │                                                       │
      │   ┌──────────┐       ┌──────────┐      ┌──────────┐   │
      │   │  metric  │◄──────┤  series  ├─────►│  label   │   │
      │   └──────────┘       └────▲─────┘      └──────────┘   │
      │                           │                           │
      │                       series_id                       │
      └───────────────────────────┼───────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │  prom_data.<metric_name>  │
                    └┬──────────────────────────┘
                     └─ BTree (series_id, time) INCLUDE (value)
```

The `_prom_catalog` schema contains metadata tables which describe `metrics`,
`label`s, and `series`. Each metric stored in the system is represented through
an entry in the `metric` table. The `label` table contains (key, value) pairs
with identity. The `series` table represents the combination of a `metric`, and
a list of `label`s.

The actual metric data is stored in a TimescaleDB hypertable (one per metric)
with the same name as the metric (assuming the metric name is shorter than 62
characters, otherwise it is truncated). These hypertables are in the
`prom_data` schema. Each row in the hypertable stores the timestamp, series id,
and the observed value.

Additional to the data in the hypertable, we build a covering btree index over
the pair of (series_id, time), including the observed value.
