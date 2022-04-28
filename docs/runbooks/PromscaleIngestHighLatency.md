# PromscaleIngestHighLatency
## Meaning
Promscale data ingestion is taking more time than expected

## Impact
Ingestion performance will be poor

## Diagnosis
1. Go to grafana dashboard about Promscale and check **Evictions** panel in **Cache** row. If you see high eviction rate, it means that Promscale caches are too small and you need to adjust it (go to [Small Promscale cache sizes](#small-promscale-cache-sizes) for mitigation steps)
2. Run query `select datname, count(application_name) from pg_stat_activity where application_name like '%promscale%' group by datname;`. This will give you the total number of Postgres connections taken by Promscale per database. This should be roughly equal to the number of CPU cores on the database machine. Go to [Low Promscale writer connections](#low-promscale-writer-connections) for mitigation steps.
3. Verify CPU and memory resource consumption of the timescale DB. Go to [Database is under high load](#database-is-under-high-load) for mitigation steps.

## Mitigation
### Database is under high load
1. Allocate more resources to the database.
2. Tune the database. Refer to [timescaledb-tune tool](https://github.com/timescale/timescaledb-tune) to do so.

### High network latency
1. Check Promscale [recommendations doc](https://github.com/timescale/promscale/blob/master/docs/configuring_prometheus.md) on `remote_write` configuration for Prometheus server
2. Adjust alert threshold according to your infrastructure

### Low Promscale writer connections
1. Increase the writer connections count. This can be done in Promscale via `-db.num-writer-connections` flag
2. If step 1 does not work, increase `-db.connections-max`

### Small Promscale cache sizes
1. The **Evictions** panel in Promscale dashboard will give the exact cache which is causing the slow down
2. Increase the cache size. See the `cache` based flags [here](https://github.com/timescale/promscale/blob/master/docs/configuration.md#metrics-specific-flags)
