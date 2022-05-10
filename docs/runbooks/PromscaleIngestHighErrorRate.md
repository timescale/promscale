# PromscaleIngestHighErrorRate

## Meaning

Promscale is experiencing high number of errors while ingesting metric samples or traces

## Impact

For metric data, high error rates will affect Prometheus remote-storage component, leading to frequent
retrying of samples batch and buffering of samples in the WAL, increasing Prometheus disk usage.
If left untreated, this will lead to upsharding, increasing Prometheus memory usage. Longer retry
durations may result dropping of samples as well.

For tracing data this may result in spans being dropped.

High error rates may also indicate an unhealthy database or a lack of any disk space.

## Diagnosis
1. Check the Promscale logs for errors. If they exists, go to [Ingestion data is invalid or corrupt](#ingestion-data-is-invalid-or-corrupt) for mitigation
2. Check the logs for Prometheus and (if used) OpenTelemetry Collector to see if there are any error messages. If any error exists, go to [Ingestion data is invalid or corrupt](#ingestion-data-is-invalid-or-corrupt) for mitigation
3. Check if your database is reachable
4. Check if the database has sufficient disk space. If the database runs out of space it can change the connection to read-only mode, leading to ingestion errors.
5. Check the Postgres logs for any errors. If found, check [Database is unhealthy](#database-is-unhealthy) for mitigation

## Mitigation

### Ingestion data is invalid or corrupt

Reconfigure your data source (Prometheus, Opentelemetry Collector, etc.) to ensure that all applied configurations
are as specified in the documentation

### Database is unhealthy
1. Ensure proper connection to the database
2. Go through Postgres logs for detailed information
3. Add more disk space to your Postgres cluster
4. Fix errors to make sure database that is able to accept connections
