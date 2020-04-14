# Data Model Schema

## Views

We define several views to make working with prometheus data easier.

### Metric Views

Metric views allows access to the full time-series prometheus data for a
given metric. By default, these views are found in the `prom_metric` schema.
Each metric has a view named after the metric name (.e.g. the `cpu_usage`
metric would have a `prom_metric.cpu_usage` view). The view contains a the
following column:

 - time - The timestamp of the measurement
 - value - The value of the measurement
 - series_id - The ID of the series
 - labels - The array of label ids
 - plus a column for each label name's id in the metric's label set

For example:
```
# \d+ prom_metric.cpu_usage
                                   View "prom_metric.cpu_usage"
    Column    |           Type           | Collation | Nullable | Default | Storage  | Description
--------------+--------------------------+-----------+----------+---------+----------+-------------
 time         | timestamp with time zone |           |          |         | plain    |
 value        | double precision         |           |          |         | plain    |
 series_id    | integer                  |           |          |         | plain    |
 labels       | integer[]                |           |          |         | extended |
 namespace_id | integer                  |           |          |         | plain    |
 node_id      | integer                  |           |          |         | plain    |
```

Example query for single point with their labels:
SELECT
    label_array_to_jsonb(labels) as labels,
    value
FROM prom_metric.cpu_usage
WHERE time < now();

```
                     labels                   | value
----------------------------------------------+-------
 {"node": "brain", "namespace": "production"} |   0.5
 {"node": "brain", "namespace": "production"} |   0.6
 {"node": "pinky", "namespace": "dev"}        |   0.1
 {"node": "pinky", "namespace": "dev"}        |   0.2
```

Example query for a rollup:

SELECT
   get_label_value(node_id) as node,
   avg(value)
FROM prom_metric.cpu_usage
WHERE time < now()
GROUP BY node_id

```
 node  | avg
-------+------
 brain | 0.55
 pinky | 0.15
```

### Series Views

The series views allows exploration of the series present for a given metric.
By default, these views are found in the `prom_series` schema. Each metric
has a view named after the metric name (.e.g. the `cpu_usage` metric would
have a `prom_series.cpu_usage` view). The view contains a the following
column:

- series_id
- labels
- plus a column for each label name's value in the metric's label set

For example:
```
# \d+ prom_series.cpu_usage
                          View "prom_series.cpu_usage"
  Column   |   Type    | Collation | Nullable | Default | Storage  | Description
-----------+-----------+-----------+----------+---------+----------+-------------
 series_id | bigint    |           |          |         | plain    |
 labels    | integer[] |           |          |         | extended |
 namespace | text      |           |          |         | extended |
 node      | text      |           |          |         | extended |
```

Example query to look at all the series:
```
# SELECT * FROM prom_series.cpu_usage;
 series_id | labels  | namespace  | node
-----------+---------+------------+-------
         4 | {3,4}   | dev        | pinky
         5 | {5,6}   | production | brain
```