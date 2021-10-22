# Downsampling

Downsampling is the ability to reduce the rate of a signal. As a result, the resolution of the data is reduced and also its size. The main reasons why this is done are cost and performance. Storing the data becomes cheaper and querying the data is faster as the size of the data decreases.

You can use two downsampling methods with Promscale: Promscale continuous aggregates and Prometheus recording rules.

## Promscale Continuous Aggregates

**Note**: this feature is currently in beta.

Promscale continuous aggregates leverage a [TimescaleDB feature of the same name](https://docs.timescale.com/timescaledb/latest/overview/core-concepts/continuous-aggregates/#refresh-cagg) to have the database manage data downsampling and materialization.

Benefits of continous aggregates:
* **Timeliness**: continuous aggregates have a feature called real-time aggregates (enabled by default) where the database automatically combines the materialized results with a query over the newest not-yet-materialized data to provide an accurate up-to-the-second view of your data.
* **Rollups**: continuous aggregates store the intermediate state of an aggregate in the materialization, making further rollups possible. Read more about the way you define aggregates in this [blog post](https://blog.timescale.com/blog/how-postgresql-aggregation-works-and-how-it-inspired-our-hyperfunctions-design-2/).
* **Query flexibility for retrospective analysis**: continuous aggregates allows for multi-purpose aggregates. For instance, Timescale’s toolkit extension has aggregates that support percentile queries on any percentile, and statistical aggregates supporting multiple summary aggregates. The aggregates that you define when you configure the materialization are much more flexible in what data you can derive at query time.
* **Backfilling**: Continuous aggregates automatically downsample all data available including past data so that you can start benefiting from the performance improvements the aggregated metric brings as soon as it is created.  



Creating a continuous aggregate in Promscale consists of two operations: creating a TimescaleDB continuous aggregate and registering the new metric so it's available to PromQL queries and other Promscale functions like the one used to configure retention. Let's use an example to illustrate how this works.

### Usage example

Let's imagine we have some metric called `node_memory_MemFree`. We can create a continuous aggregate to derive some summary statistics (min, max, average) about the reading on an hourly basis. To do it we will run the following query on the underlying TimescaleDB:

```
CREATE MATERIALIZED VIEW node_memfree_1hour( time, min, max, avg, count, sum, series_id)
WITH (timescaledb.continuous) AS
  SELECT
        time_bucket('1 hour', time) + '1 hour' as time ,
        series_id,
        min(value) as min,
        max(value) as max,
        avg(value) as avg
    FROM prom_data.node_memory_MemFree
    GROUP BY time_bucket('1 hour', time), series_id
```

For Promscale to be able to use a continuous aggregate as a metric view it must meet the following requirements:
* It must be based on a raw metric series ingested by Promscale that is specified in the FROM clause.
* It must include a column named `time` of type `timestamptz` that corresponds to the time associated to each aggregated metric sample. Note we add 1 hour to time_bucket in the SELECT clause to match the PromQL semantics of representing a bucket with the timestamp at the end of the bucket instead of the start of the bucket.
* It must include a column named `series_id` of type bigint that corresponds to the series_id from the raw metrics.
* A number of additional columns of type `double precision`  that correspond to the metric values you want to store.

Currently continuous aggregates only supports one metric metric series in the FROM clause and can only generate aggregations within the same series_id (a series_id corresponds to a specific metric name and set of labels).

For more information on continuous aggregates and all their options, refer to the [documentation](https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/). This continuous aggregate can now be queried via SQL. To make it possible to query the data with PromQL, we have to register it with Promscale as a metric view:

```
SELECT register_metric_view('public', 'node_memfree_1hour');
```

The two arguments represent the view schema and the view name in Postgres. Now, we can treat this data as a regular metric in both SQL and PromQL.

#### Querying the new data

To query the average in the new aggregated metric with SQL and show it in a time series chart in Grafana you would write the following query:

```
SELECT time, jsonb(labels) as metric, avg
FROM node_memfree_1hour
WHERE $__timeFilter(time)
ORDER BY time asc
```
To do the same with PromQL the query would be

```
node_memfree_1hour{__column__="avg"}
```

Note the use of the special `__column__` label. The aggregated metric actually holds multiple metrics (min, mav and avg) in different columns which is not something Prometheus supports (Prometheus stores all metrics independently even for things like summaries or histograms). Promscale uses that label to identify what column to return. When no `__column__` label is specified, Promscale returns the `value` column by default. When ingesting Prometheus metrics Promscale uses the value column and that's why you don't have to specify a `__column__` label in your queries.

The above aggregate could be modified to name one of the metrics as `value` to make Promscale return that metric when no `__column__` label is specified. Reusing the previous example we create a continuous aggregate that sets the average as the default to be returned in PromQL queries.

```
CREATE MATERIALIZED VIEW node_memfree_1hour( time, min, max, avg, count, sum, series_id)
WITH (timescaledb.continuous) AS
  SELECT
        time_bucket('1 hour', time) + '1 hour' as time ,
        series_id,
        min(value) as min,
        max(value) as max,
        avg(value) as value
    FROM prom_data.node_memory_MemFree
    GROUP BY time_bucket('1 hour', time), series_id
```

Now to query the average you would use a query with no `__column__` label:

```
node_memfree_1hour
```

which is equivalent to

```
node_memfree_1hour{__column__="value"}
```

Promscale also adds a new tag called `__schema__` which will help you identify in which schema the metric view is registered. Generally you would not need to use this but in cases where you may have two metrics with the same name in different schemas (which we strongly discourage but it may happen by mistake), you would need to select which one you want to use. If you don't, then Promscale will query the metric in the prom_data schema which is the one used for all ingested metrics. An easy way to avoid those issues would be to make sure you name your continuous aggregate views with a name that raw ingested metrics will not have, like `node_memfree_**1hour**`.

Finally, note that both the `__schema__` and `__column__` label matchers support only exact matching, no regex or other multi value matchers allowed. Also, metric views are excluded from queries that match multiple metrics (i.e., matching on metric names with a regex).

```
{__name__=~"node_mem*"} // this valid PromQL query will not match our previously created metric view
```

### Deleting a Continuous Aggregate

To delete a Promscale continuous aggregate, you have to delete the metric view and then remove the continuous aggregate.

So continuing with our example, we would first remove the metric view:

```
unregister_metric_view('public', 'node_memfree_1hour');
```

And then we would delete the continuous aggregate:

```
DROP MATERIALIZED VIEW node_memfree_1hour;
```

## Prometheus Recording Rules

Prometheus provides an out-of-the-box downsampling mechanisms called recording rules which are used to pre-calcule frequently used or computationally expensive queries. A recording rule is a PromQL expression that Prometheus evaluates at a predefined frequency to generate a new metric series. The new metric series will be stored in Promscale and you will be able to query it as any other Prometheus metric.

This is an example of a recording rule:

```
groups:
  - name: daily_stats
    interval: 1h
    rules:
    - record: customer:api_requests:rate1day
      expr: sum by (customer) (rate(api_requests_total[1d]))

```

For the recording rules to be applied, you need to point to your recording rules file in the Prometheus the configuration file:

```
rule_files:
  - "<recording-rules-file>"
```

To query the metric with PromQL you could do

```
customer:api_requests:rate1day
```

And with SQL;

```
SELECT time, jsonb(labels) as metric, value
FROM "customer:api_requests:rate1day"
ORDER BY time asc
```

To learn more about recording rules check the [Prometheus documentation](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/).

**Note**: We recommended setting `read_recent` to `true` in the [Prometheus remote_read configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) when using recording rules. This tells Prometheus to fetch data from Promscale when evaluating PromQL queries (including recording rules). If `read_recent` is disabled, **only** the data stored in Prometheus's local tsdb will be used when evaluating alerting/recording rules and thus be dependent on the retention period of Prometheus (not Promscale).

## Data Retention

All metrics in Promscale use the default retention period when they are created but you can change both the default retention period as well as the retention period for individual metrics which Prometheus doesn't provide. A typical use case is to retain aggregated metrics for longer for trend analysis.

Read more in the [metric retention documentation](metric_deletion_and_retention.md#metric-retention).

## Choosing a downsampling method

There are a few things to take into account when deciding a downsampling solution:

* **Access to recent data**. If this materialization will be used in operational or real-time dashboards prefer continuous aggregates because of the  real-time aggregate feature.
* **Size of the time-bucket**. Continuous aggregates materialize the intermediate, not the final form, so querying the data is a bit more expensive than with recording rules. Thus, continuous aggregates are better when aggregating more data points together (1 hour or more of data), while recording rules are better for small buckets.
* **Number of metrics in materialization**. Currently, continuous aggregates can only be defined on a single metric. If you need to materialize queries on more than one metric, use recording rules. However, you should also consider whether joining the materialized metrics (the result of the materialization instead of the raw input) may be a better approach.
* **Query flexibility**. If you know the exact queries that will be run on the materialization, recording rules may be more efficient. However, if you want flexibility, continuous aggregates can answer more queries based on the materialized data.
* **Access to old data**. If you need old data points to also be aggregated as soon as downsampling for a metric is configured, continuous aggregates would be a better choice, especially if this is something you think you will be doing often, since recording rules require additional steps to backfill data.
