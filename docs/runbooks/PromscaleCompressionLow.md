# PromscaleCompressionLow

## Meaning

Promscale contains high uncompressed data

## Impact

High disk usage by Promscale database

## Diagnosis
1. Open Grafana and navigate to Promscale dashboard
2. Go to Database section and see `Compressesd chunks ratio`. If you see a ratio of < 10% then compression is not adequate in your system
3. Open psql
4. Check number of uncompressed chunks: `select count(*)::bigint from _timescaledb_catalog.chunk where dropped=false and compressed_chunk_id=null;`
5. Check number of maintenancec jobs: `select count(*) from timescaledb_information.jobs where proc_name = 'execute_maintenance_job'`
6. Run the following debugging query:

```postgresql
SELECT 
     avg(chunk_interval) as avg_chunk_interval_new_chunk,
     sum(total_size_bytes-coalesce(after_compression_bytes,0)) 
         AS not_yet_compressed_bytes,
     avg(total_interval-compressed_interval) 
         AS avg_not_yet_compressed_interval,     
     avg(total_chunks-compressed_chunks) AS avg_not_yet_compressed_chunks,
     sum(total_chunks-compressed_chunks) AS total_not_yet_compressed_chunks,
     count(*) total_metrics
FROM  prom_info.metric;
```

If `avg_not_yet_compressed_interval > 2` then See [Lots of uncompressed chunks](PromscaleMaintenanceJobRunningTooLong.md#lots-of-uncompressed-chunks) for mitigation

If `avg_not_yet_compressed_interval is high and avg_not_yet_compressed_chunks is small (<2)` then it indicates very big chunks.
This can lead to maintenance jobs stuck on compressing this large chunk or waiting for the current chunk to stop getting new data.
See [Very large uncompressed chunks](PromscaleMaintenanceJobRunningTooLong.md#very-large-uncompressed-chunks) for mitigation

7. Check number of databases: `SELECT count(*) from pg_catalog.pg_database`
8. Check max number of background workers: `SELECT current_setting(timescaledb.max_background_workers);`
9. Make sure that **max number of background workers > number of databases + 2**.
   If not, see [Low background workers limit compared to available databases](#low-background-workers-limit-compared-to-available-databases) for mitigation
10. Run: `SELECT * FROM timescaledb_information.job_stats js INNER JOIN timescaledb_information.jobs j ON (js.job_id = j.job_id) WHERE proc_name ='execute_maintenance_job'`
    See **Mitigation** in [PromscaleMaintenanceJobFailures](PromscaleMaintenanceJobFailures.md#mitigation) for more info

Note: When in addition to [PromscaleCompressionLow](PromscaleCompressionLow.md) alert, if you have a [PromscaleMaintenanceJobFailures](PromscaleMaintenanceJobFailures.md) alert firing,
please solve the latter one first by following its runbook.

## Mitigation

### Low number of maintenance jobs

Increase background jobs using: `SELECT prom_api.config_maintenance_jobs(number_jobs=> <new_jobs_count>)`

### Low background workers limit compared to available databases

Increase background workers limit:
1. Get current background workers limit: `SELECT current_setting(timescaledb.max_background_workers);`
2. Increment background workers limit: `ALTER system set timescaledb.max_background_workers to '<new workers limit>';`
3. Reload postgres
