# Prometheus HTTP API reference

The [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) defines endpoints
for executing PromQL expression queries (instant and range), query metadata about the series and labels, retrieve
information about alerts, targets and the complete configuration of the prometheus server.

We implemented the endpoints for executing PromQL and labels metadata to allow the Promscale connector to be
used as a Prometheus compatible data-source in applications like Grafana.

<img src="assets/promscale-arch.png" alt="Promscale Architecture Diagram" width="800"/>

When requesting a query to run on data that is in the long-term storage through Prometheus, the query goes through the
following steps.
1. The user issues a query to the prometheus server
2. Query engine requests the data through the remote_read protocol;
3. The connector translates the request to SQL statement with only a time range and label matchers applied;
4. The database returns almost raw data;
5. The connector marshals it into a format compatible with the remote_read protocol;
6. The Query engine combines the local and remote data and applies any functions or aggregations before returning a
   result.

By having the Connector implement the PromQL APIs, the connector can:
1. The user issues a query directly to the connector
2. Parse the PromQL and translate it to a SQL statement that with a time range, label matchers, calculations and
   aggregates to be pushed down;
3. The database executes the query;
4. A slightly modified Prometheus Query Engine (to leverage more pushdowns) finalizes the calculation and marshals the
   data to a proper response.

By using the Connector for PromQL queries directly a network trip is avoided, and TimescaleDB is better utilized to
actually perform some calculations.

## Implemented Endpoints

| Name                                                                                                 | Endpoint                                    | Description                                                |
|------------------------------------------------------------------------------------------------------|---------------------------------------------|------------------------------------------------------------|
| [Instant Queries](https://prometheus.io/docs/prometheus/latest/querying/api#instant-queries)         | `GET,POST /api/v1/query`                    | Evaluate an instant query at a single point in time        |
| [Range Queries](https://prometheus.io/docs/prometheus/latest/querying/api#range-queries)             | `GET,POST /api/v1/query_range`              | Evaluate an expression query over a range of time          |
| [Series](https://prometheus.io/docs/prometheus/latest/querying/api#finding-series-by-label-matchers) | `GET,POST /api/v1/series`                   | Return a list of time series that match a label set        |
| [Label Names](https://prometheus.io/docs/prometheus/latest/querying/api#getting-label-names)         | `GET,POST /api/v1/labels`                   | Return a list of label names                               |
| [Label Values](https://prometheus.io/docs/prometheus/latest/querying/api#querying-label-values)      | `GET /api/v1/label/<label_name>/values`     | Return a list of label values for a provided label name    |
| [Delete Series](https://prometheus.io/docs/prometheus/latest/querying/api#delete-series)             | `PUT,POST /api/v1/admin/tsdb/delete_series` | Deletes sets whose label_set matches the provided matchers |
| [Exemplar Queries](https://prometheus.io/docs/prometheus/latest/querying/api#querying-exemplars)     | `GET,POST /api/v1/query_exemplars`          | (Experimental) Evaluate an expression query for Exemplars  |
