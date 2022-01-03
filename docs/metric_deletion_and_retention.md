# Deleting Data

Deletion of data in Promscale can be categorized under:

1. Deletion of series
2. Deletion of metric
3. Deletion of data by time
4. Metric retention (Deletion via scheduling data retention policies (SQL))

Promscale supports series deletion & metric retention. Series deletion deletes all data for a specified series over the
entire time-range.

Metric retention deletes data older than a specified duration for a given metric. You can specify a default metric
retention period as well as overwrite that default on a particular metric.

## Deletion of series (HTTP API)

Promscale exposes an HTTP API for deletion of series, simply by using the label_matchers. This works the same as the Prometheus delete HTTP API.
However, for deleting data in Promscale, you will need to enable permissions for advanced users. This is done by setting
[`web-enable-admin-api`](https://github.com/timescale/promscale/blob/master/docs/cli.md#general-flags) flag to `true`.

**Note**: The start and end timestamp options are not supported yet. Currently, the `delete_series` HTTP API lets you
delete the series across the entire time-range only.

#### HTTP API

URL query parameters:

* **match[]=<series_selector>**: Repeated label matcher argument that selects the series to delete. At least one match[] argument must be provided.

```
POST /delete_series
PUT /delete_series
```

**Example:**

For deleting series that have labels as `job="prometheus"` and a regex label `instance="prom.*"`.

```shell
curl -X POST -g http://localhost:9201/delete_series?match[]={job="prometheus", instance=~"prom.*"}
```

## Deletion of metric

Promscale allows you to delete an entire metric both via SQL and HTTP API.

### SQL

You can delete an entire metric from the database using the function `prom_api.drop_metric(metric_name_to_be_dropped text)`.
This is an administrative command and will work only when no Promscale instances are attached to the database.

**Example:**

In order to delete the metric `container_cpu_load_average_10s`

```postgresql
SELECT prom_api.drop_metric('container_cpu_load_average_10s');
```

### HTTP API

In order to delete all the data in the metric via the HTTP API, you can use the `/delete_series` endpoint and pass the metric name to 
be deleted, as the matcher. Please note that this is different from dropping the metric as shown in the SQL above since 
this will delete all the data but leave the metric itself. Moreover, for deleting data in Promscale,
you will need to enable permissions for advanced users. This is done by setting
[`web-enable-admin-api`](https://github.com/timescale/promscale/blob/master/docs/cli.md#general-flags) flag to `true`.

URL query parameters:

* **match[]=<series_selector>**: Repeated label matcher argument that selects the series to delete. At least one match[] argument must be provided.

```
POST /delete_series
PUT /delete_series
```

**Example:**

In order to delete all the data in the metric `container_cpu_load_average_10s` using the `/delete_series` HTTP API

```shell
curl -X POST -g http://localhost:9201/delete_series?match[]=container_cpu_load_average_10s
```

## Deletion of data by time (SQL)

Deletion of series or metrics based on time can be done only through SQL. The support for time-based deletion in the
`/delete_series` endpoint will be added in near future.

### SQL

Since data inside hypertable can be present in form of compressed and uncompressed chunks, we need to first decompress
all the compressed chunks and then perform any time-based deletion. We may recompress the chunks after deletion if required.

**Example:**

In order to delete all data from `container_cpu_load_average_10s` metric that is beyond 10 hours of time,

Lets decompress all chunks related to the hypertable of the given metric.

```postgresql
SELECT decompress_chunks(show_chunks('prom_data.container_cpu_load_average_10s'));
```

Then, we perform the deletion query.

```postgresql
DELETE FROM prom_data.container_cpu_load_average_10s WHERE time > Now() - interval '10 hour';
```

Note: If you want to delete a particular series from that metric only, you can mention `series_id=<id>` in `WHERE`
clause in the above `DELETE` query.

Now, we can recompress the remaining data.

```postgresql
SELECT compress_chunks(show_chunks('prom_data.container_cpu_load_average_10s', older_than => '2 hours'));
```

## Metric Retention

Promscale offers full control over data retentions i.e., you can set a default data retention period as well as overwrite the default on a per-metric basis.

Get the default retention policy

```postgresql
SELECT EXTRACT(day FROM _prom_catalog.get_metric_retention_period(''))
```

Get the default retention period, specific to a metric

```postgresql
SELECT EXTRACT(day FROM _prom_catalog.get_metric_retention_period('container_cpu_usage_seconds_total'))
```

Set the default retention policy to two days for all metrics that do not have custom retention policy already configured.

```postgresql
SELECT prom_api.set_default_retention_period(INTERVAL '1 day' * 2)
```

Set a custom retention policy for a specific metric.

```postgresql
SELECT prom_api.set_metric_retention_period('container_cpu_usage_seconds_total', INTERVAL '1 month')
```

Reset specific metric retention to the default metric retention.

```postgresql
SELECT prom_api.reset_metric_retention_period('container_cpu_usage_seconds_total')
```

For TimescaleDB versions < 2.0, the retention policies are executed using a cron job. Please see the Promscale
installation instructions for your platform to see how to set up the cron job.
