# Data Model Schema

## Views

We define several views to make working with prometheus data easier.

### Metric Views

Metric views allows access to the full time-series prometheus data for a
given metric. By default, these views are found in the `prom_metric` schema.
The `prom_metric` view is added to your search_path when you first install
timescale_prometheus and so it is default view you see if you don't
schema-qualify a view name. Each metric has a view named after the metric
name (.e.g. the `cpu_usage` metric would have a `prom_metric.cpu_usage` or
simply `cpu_usage` view). The view contains a the following column:

 - time - The timestamp of the measurement
 - value - The value of the measurement
 - series_id - The ID of the series
 - labels - The array of label ids
 - plus a column for each label name's id in the metric's label set

For example:
```
# \d+ cpu_usage
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
FROM cpu_usage
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
FROM cpu_usage
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

## Series Selectors

We have added simple-to-use series selectors for filtering series in either of the two views above.

### Containment

To test whether a series contains a json fragment you can use the `@>` containment operator.

For example to find all metrics on in the dev namespace and on node pinky, run:

```SQL
SELECT *
FROM prom_series.cpu_usage  u
WHERE labels @> jsonb '{"namespace":"dev", "node": "pinky"'}
```

```
 series_id | labels  | namespace  | node  | region
-----------+---------+------------+-------+--------
         4 | {3,4,5} | dev        | pinky | East
         5 | {3,4,5} | dev        | pinky | West
```

### Label Matchers

You can also match series using label matchers to create predicates on values of particular
label keys. This is very similar to the label matchers available in PromQL.

For example to find all metrics on in the dev namespace and on node brain using
label matcher, you can run:

```SQL
SELECT *
FROM cpu_usage  u
WHERE labels ? ('namepace' == 'dev') AND labels ? ('node' == 'brain')
```

Label matchers are formed by using a qualifier of the form `labels ? (<tag_key> <operator> <pattern>)`.
There are four operators,

- `==` match tag values that are equal to the pattern
- `!==` match tag value that are not equal to the pattern
- `==~` match tag values that match the pattern as a regex
- `!=~` match tag values that are not equal to the pattern

These four matchers correspond to each of the four selectors in PromQL but with slightly
different names (to avoid clashing with other PostgreSQL operators). They can
be combined together using any boolean logic with any arbitrary where clauses.

For those coming from PromQL there are a few differences to keep in mind:
- Regexes are not anchored for you. Although, you can of course add anchors (`^$`) yourself.
- The logic for whether series that are missing the tag key pass the qualifier is slightly different:
  If the key on the left-hand side is not found `!==` and `!=~` always match, while `==` and `==~` never match.


### Equivalence

The `eq` function tests exact equivalence between labels, without comparing the metric name (`__name__`) label key.
For instance if the labels `a` is `{"__name__":"metric", "foo":"bar", "baz":"frob"}`
then `SELECT eq(a, jsonb {"__name__":"something else", "foo":"bar", "baz":"frob"})` will evaluate to `true`, however, unlike `@>` if `SELECT eq(a, {"__name__":"metric", "foo":"bar"})` will evaluate to `false`.
Thus, it can be used to compare across metrics or within a metric.

For example, to join 2 series that are scraped at the same time:

```SQL
SELECT *
FROM cpu_usage  u
INNER JOIN cpu_total t  ON (u.time=t.time AND eq(u.labels, t.labels))
WHERE u.labels ? ('namespace' == 'dev') AND u.labels ? ('node' ==~ 'pin*')
```

You can also use eq to compare to a json labels object:

```SQL
SELECT *
FROM cpu_usage
WHERE eq(labels , jsonb '{“namespace”:”prod”,”node”:”pinky”,"zone":"us-a1-east"}')
```

Note the eq function tests equivalence of the entire label object.
Therefore you need to provide the entire json object if using the
function above. For partial matches see the Containment
section above.
