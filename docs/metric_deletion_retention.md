# Deleting Data

Promscale supports series deletion & metric retention. Series deletion deletes all data for a specified series over the entire time-range. Metric retention deletes data older than a specified duration for a given metric. You can specify a default metric retention period as well as overwrite that default on a particular metric.

## Series Deletion

Promscale exposes an API for deletion of series. This works same as prometheus delete API.

**Note**: The start and end timestamp options are not supported yet. Currently, the `delete_series` API lets you delete the series across the entire time-range.

#### Promscale Delete API

URL query parameters:

* **match[]=<series_selector>**: Repeated label matcher argument that selects the series to delete. At least one match[] argument must be provided.

```
POST /delete_series
PUT /delete_series
```

Example:

```
curl -X POST -g http://promscale:9201/delete_series?match[]=container_cpu_usage_seconds_total
```

## Metric Retention

TimescaleDB offers full control over data retentions i.e you can set a default data retention period as well as overwrite the default on a per-metric basis.

SQL command to set the default retention policy to two days for all metrics that do not have custom retention policy already specified.

```
SELECT prom_api.set_default_retention_period(INTERVAL '1 day' * 2)
```

SQL command to set a custom retention policy for a specific metric.

```
SELECT prom_api.set_metric_retention_period(container_cpu_usage_seconds_total, INTERVAL '1 day' * 2)
```

SQL command to reset specific metric retention to the default metric retention.

```
SELECT prom_api.reset_metric_retention_period(container_cpu_usage_seconds_total)
```

The retention policies are executed using a cron job. Please see the Promscale installation instructions for your platform to see how to set up the cron job.
