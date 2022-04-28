# PromscaleDBHighErrorRate
## Meaning
Promscale is experiencing high errors while performing database requests

## Impact
Ingestion and querying operations will have frequent failures. This may also lead to loss of data while ingesting traces.

## Diagnosis
1. Make sure that Prometheus is monitoring Promscale's `/metric` endpoint
2. Make sure that Promscale Dashboard is installed on Grafana
3. Open Grafana
4. Open Promscale dashboard
5. Go to Query section
6. See the **Errors (HTTP)** graph. If you see high error rates, see [Invalid or corrupt query data](#invalid-or-corrupt-query-data) for mitigation
7. Check Postgres logs to see if there are any errors. If found, see [Database is unhealthy](#database-is-unhealthy) and [Ingestion data is invalid or corrupt](#ingestion-data-is-invalid-or-corrupt) for mitigation

## Mitigation
### Database is unhealthy
1. Check the logs and fix errors (if any)
2. Make sure that database is ready for normal operations
3. Add more disk space if required

### Ingestion data is invalid or corrupt
Same as **Ingestion data is invalid or corrupt** in [PromscaleIngestHighErrorRate](PromscaleIngestHighErrorRate.md#ingestion-data-is-invalid-or-corrupt).

### Invalid or corrupt query data
Make sure your queries do not contain any corrupt information and is parsable by SQL
