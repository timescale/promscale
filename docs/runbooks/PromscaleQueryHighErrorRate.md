# PromscaleQueryHighErrorRate
## Meaning
Promscale is experiencing high error rates while evaluating queries

## Impact
Frequent query evaluation failures. Alerts based on data retrieved via Promscale may not be accurate

## Diagnosis
1. Check query errors in the grafana dashboard in **Errors (HTTP)** in the Query row. If the panel show high error rate, go to [Invalid PromQL query](#invalid-promql-query) for mitigation steps
2. Check if your database is reachable and accepting queries by running a simple query `select time, value from promscale_query_requests_total limit 1;`
3. Go to Grafana and open Promscale dashboard. Go to database health panel and see if it shows high error rates. If the error rate is not high, then the alerts are specific to the queries executed. Otherwise, see [Database is unhealthy](#database-is-unhealthy) for mitigation
4. Check database logs for errors
5. Go to [Database is unhealthy](#database-is-unhealthy) for mitigation steps

## Mitigation
### Database is unhealthy
1. Ensure database has proper resources
2. Check Postgres logs and fix errors if any

### Invalid PromQL query
1. Enable prometheus query log using this [guide](https://prometheus.io/docs/guides/query-log/#enable-the-query-log)
2. Check the query log for failing PromQL queries
3. Check the PromQL query syntax that is failing your grafana dashboard or recording rules
4. Run the above PromQL query and verify if it can execute in a reasonable time
