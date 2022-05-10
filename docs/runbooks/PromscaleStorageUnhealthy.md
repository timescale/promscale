# PromscaleStorageUnhealthy

## Meaning

Promscale database is unhealthy with frequent errors while checking the health

## Impact

Ingestion and querying operations can witness frequent failures, timeouts or may take more time to complete

## Diagnosis

Storage unhealthy alert is fired when the `/healthz` endpoint does not report success for a significant duration of time.
Check Postgres logs and see if there are any errors

## Mitigation

Refer to the Mitigation for **Database is unhealthy** in [PromscaleIngestHighErrorRate](PromscaleIngestHighErrorRate.md#database-is-unhealthy)
