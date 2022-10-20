# PromscaleMaintenanceJobRunningTooLong

## Meaning

Promscale maintenance jobs are taking longer than expected to complete

## Impact

Delay in compressing chunks and executing retention policy, leading to high disk usage and data past retention period

## Diagnosis
1. Open psql
2. Execute the following debugging query:

```postgresql
SELECT
    avg(chunk_interval) as avg_chunk_interval_new_chunk,
    sum(total_size_bytes-coalesce(after_compression_bytes,0)) AS not_yet_compressed_bytes,
    avg(total_interval-compressed_interval) AS avg_not_yet_compressed_interval,     
    avg(total_chunks-compressed_chunks) AS avg_not_yet_compressed_chunks,
    sum(total_chunks-compressed_chunks) AS total_not_yet_compressed_chunks,
    count(*) total_metrics
FROM  prom_info.metric;
```

If `avg_not_yet_compressed_chunks > 2` then See [Lots of uncompressed chunks](#lots-of-uncompressed-chunks) for mitigation

If `avg_not_yet_compressed_interval is high AND avg_not_yet_compressed_chunks is small (<2)` then it indicates very big chunks.
This can lead to maintenance jobs stuck on compressing this large chunk or waiting for the current chunk to stop getting new data.
See [Very large uncompressed chunks](#very-large-uncompressed-chunks) for mitigation.

3. Open Postgres logs and look for errors or deadlocks. If found, see [Deadlock/bug in background jobs](#deadlockbug-in-background-jobs) for mitigation

## Mitigation

### Lots of uncompressed chunks
1. An immediate solution to this is manually compressing chunks by calling: `call prom_api.execute_maintenance();`. This will also perform retention policies.
2. Try increasing the number of maintenance jobs by using: `SELECT prom_api.config_maintenance_jobs(number_jobs => X, new_schedule_interval => '30 minutes'::interval)`. The given `number_jobs` will run every `new_schedule_interval` minutes (in this case, 30). Maintenance jobs are responsible for performing compression and applying retention policies.

### Very large uncompressed chunks

Decrease the chunk interval.

1. Get the current interval by:
   1. Seeing the Promscale configuration file
      Or
   2. Running SQL: `select value from _prom_catalog.default where key = 'chunk_interval';`
2. Update the interval:
   1. [Updating Promscale configuration](https://github.com/timescale/promscale/blob/master/docs/dataset.md)

### Deadlock/bug in background jobs

If you find any errors in Postgres logs or anything related to this topic, please open an issue at https://github.com/timescale/promscale/issues
