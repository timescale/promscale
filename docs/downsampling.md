# Downsampling with Promscale and coninouous aggregates

Downsampling is the ability to reduce the rate of a signal. As a result, the resolution of the data is reduced and also its size. The main reasons why this is done are cost and performance. Storing the data becomes cheaper and querying the data is faster as the size of the data decreases.

In the Prometheus ecosystem, downsampling is usually done through [recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/). These rules operate on a fairly simple mechanism: on a regular, scheduled basis the rules engine will run a set of user-configured queries on the data that came in since the rule was last run and will write the query results to another configured metric. So if we have a metric called `api_requests` we can define a metric rule for a new metric called `customer:api_requests:rate1day` and configure the rules engine to calculate the rate of API requests every 24 hours and write the result to the new metric. 

Promscale supports another downsampling method called continuous aggregation which is more timely and accurate than recording rules in many circumstances. Combined, these two methods cover the majority of downsampling use-cases.

## Function index

 Name | Arguments | Return type | Description
 --- | --- | --- | ---
 register_metric_view           | schema_name text, view_name text, if_not_exists boolean                                                        | boolean                  | Register metric view with Promscale. This will enable you to query the data and set data retention policies through Promscale. Schema name and view name should be set to the desired schema and view you want to use. Note: underlying view needs to be based on an existing metric in Promscale (should use its table in the FROM clause). 
 unregister_metric_view           | schema_name text, view_name text, if_not_exists boolean                                                        | boolean                  | Unregister metric view with Promscale. Schema name and view name should be set to the metric view already registered in Promscale. 

### Simple usage example

Let's imagine we have some metric called `node_memory_MemFree`. We can create a continuous aggregate to derive some summary statistics (min, max, average) about the reading on an hourly basis. To do it we will run the following query on the underlying TimescaleDB database which requires using any tool that can connect to PostgreSQL and execute queries like [psql](https://docs.timescale.com/timescaledb/latest/how-to-guides/connecting/psql/).

Example on how to create the continuous aggregate from our raw metric:

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

For more information on continuous aggregates and all their options, refer to the [documentation](https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/). This continuous aggregate can now be queried via SQL and we can also make it available to PromQL queries (as a reminder, Promscale is [100% PromQL compliant](https://promlabs.com/blog/2021/10/14/promql-vendor-compatibility-round-three)). To do this we have to register it with Promscale as a PromQL metric view:

```
SELECT register_metric_view('public', 'node_memfree_1hour');
```

The two arguments represent the view schema and the view name in Postgres. Now, we can treat this data as a regular metric in both SQL and PromQL.

### Querying the new data

While the new data is queriable with SQL, the more interesting feature is how you access it using PromQL. Running the query:

```
node_memfree_1hour{__column__="avg"}
```

will now return the value of the "avg" column in our newly created continuous aggregate in the form you would expect for a PromQL query. It will return all the exact tags that the raw metric has set plus a couple of additional ones.

### Additional tags in querying

Promscale introduces two new tags called "__schema__" and "__column__". These are used to query for the metric view in a specific schema and column of that view. They are also set in the results of the queries on metric views.

### Considerations and gotchas

Before using Promscale continuous aggregates there are a few considerations to take into account.

First, if the `__column__` label matcher is not specified, it will default to `value`, which means it will try to query the column named `value`. If the column does not exist, we will get an empty result (since it won't match anything in the system). To take advantage of this fact, consider creating continuous aggregates with a `value` column set to the value you want as the default value when matching the metric. In our `node_memfree_1hour` example, we could have used the following continuous aggregate instead:

```
CREATE MATERIALIZED VIEW node_memfree_1hour( time, min, max, avg, series_id)
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

With this configuration, getting the value of the average of `node_memfree_1hour` with PromQL would simply be

```
node_memfree_1hour
```

So no need to pass a `__column__` selector.

Second, both the `__schema__` and `__column__` label matchers support only exact matching, no regex or other multi value matchers allowed. Also, metric views are excluded from queries that match multiple metrics (i.e., matching on metric names with a regex).

```
{__name__=~"node_mem*"} // this valid PromQL query will not match our previously created metric view
```

Finally, if we ingest a new metric with the same name as a registered metric view, it will result in the creation of a new metric with the same name but a different schema (all ingested metrics are automatically added to the `prom_data` schema, while the new aggregated metric would be in the public schema by default). This would likely cause confusion when querying a metric by its name since by default Promscale will query the metric in the prom_data schema (we could specify a different `__schema__` label in our query). To avoid it, make sure you name your continuous aggregate views with a name that raw ingested metrics will not have, like `node_memfree_1hour`.
