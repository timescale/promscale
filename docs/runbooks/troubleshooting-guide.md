# Troubleshooting Guide

## General information to always gather

This is a basic set of information that is almost always relevant to troubleshooting.

* Determine the version of Postgres, TimescaleDB, the Promscale connector, and the Promscale extension.
* Determine the chunk policy: `select _prom_catalog.get_default_chunk_interval();`
* Determine the retention policy: `select _prom_catalog.get_default_retention_period();`
* Determine the infrastructure setup for TimescaleDB and Promscale. Does anything else share compute or storage resources (do other things run on the same infrastructure?)?
* If using Prometheus (run PromQL queries against Promscale if you have multiple Prometheus instances)
  * Number of Prometheus instances
  * Total metric ingest rate (if you are ingesting metrics): `sum(rate(prometheus_tsdb_head_samples_appended_total[5m]))`
  * Total active series: `prometheus_tsdb_head_series`
  * Series churn: `sum(rate(prometheus_tsdb_head_series_created_total[5m]))`
* If using OpenTelemetry traces:
  * Span ingest rate: `SELECT count(*)/600 as span_throughput FROM ps_trace.span WHERE start_time > NOW() - INTERVAL '10m';`

## Background Jobs / Compression Issues

Check how many chunks are compressed / uncompressed:

```SQL
SELECT is_compressed,count(*) 
FROM timescaledb_information.chunks 
WHERE hypertable_schema='prom_data' 
GROUP BY is_compressed;
```

Usually, if background jobs are taking a very long time and consuming a lot of 
resources it happened in versions 0.10.0 and earlier as the number of chunks in
the database grew. Upgrading to 0.11.0 or later should improve things significantly. 

You can check [pg_stat_statements](https://www.postgresql.org/docs/current/pgstatstatements.html)
in the database to see what statements are taking most time to execute. Are they 
related to compression, or to retention?
```SQL
-- pg_stat_activity shows the current activity of database clients
SELECT * FROM pg_stat_activity;
-- pg_stat_statements shows performance info for queries that have been run
SELECT * FROM pg_stat_statements order by total_exec_time DESC;
```
The info in pg_stat_statements is cumulative. If you want to reset that data to 
start with a clean slate, run this:
```SQL
SELECT pg_stat_statements_reset();
```

Check if there is lock contention with the following queries:
```SQL
select * from pg_locks where not granted;

select 
  pid,
  usename,
  pg_blocking_pids(pid) as blocked_by,
  query as blocked_query
from pg_stat_activity
where cardinality(pg_blocking_pids(pid)) > 0;
```
