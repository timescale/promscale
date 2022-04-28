# PromscaleCacheHighNumberOfEvictions
## Meaning
Promscale is evicting cache entries at a higher rate than expected

## Impact
Ingestion and query operations have poor throughput performance

## Diagnosis
1. Open Promscale dashboard in Grafana
2. Go to Evictions chart in Cache section
3. If the evictions is high, see [High metric series](#high-metric-series) for mitigation

## Mitigation
### High metric series
1. Increase the cache size based on the requirement.
2. See the `cache` based flags [here](https://github.com/timescale/promscale/blob/master/docs/configuration.md#metrics-specific-flags)
