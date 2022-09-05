# PromscaleMaintenanceJobFailures

## Meaning

Promscale maintenance jobs failed to execute successfully

## Impact

Delay in compression and retention policy, leading to high disk usage

## Diagnosis
1. Open psql
2. Run the following debugging query:

```postgresql
select * from
    timescaledb_information.job_stats stats
inner join
    timescaledb_information.jobs jobs
        on jobs.job_id = stats.job_id
where jobs.proc_name = 'execute_maintenance_job';
```

This will give you information about the maintenance jobs. The `last_run_status` column will indicate any failed jobs.
For mitigation, see [Unexpected maintenance jobs behaviour](#unexpected-maintenance-jobs-behaviour)

3. Check Postgres logs for any failure

## Mitigation

### Unexpected maintenance jobs behaviour

Run the following debugging query

```postgresql
SELECT * FROM timescaledb_information.job_stats js 
    INNER JOIN timescaledb_information.jobs j USING (job_id) 
WHERE proc_name ='execute_maintenance_job';
```

If the output has:
1. no rows => No jobs are installed

`SELECT prom_api.config_maintenance_jobs(number_jobs => 2, new_schedule_interval => '30 minutes'::interval)`

2. last_run_status not = 'Success' => Job had an error

Run `CALL prom_api.execute_maintenance(true);` and see if there are any obvious errors

3. job_status != 'Scheduled' => Job disabled

4. next_start not within 30 min

Try increasing maintenance jobs and see if it helps: `SELECT prom_api.config_maintenance_jobs(number_jobs => X, new_schedule_interval => '30 minutes'::interval)`

If some problem persists with scheduling, then please open an issue at https://github.com/timescale/promscale/issues

5. last_run_duration is high (>30 min) => Not enough parallelism

Increase the number of maintenance jobs N using `SELECT prom_api.config_maintenance_jobs(number_jobs => N, new_schedule_interval => '30 minutes'::interval)`
