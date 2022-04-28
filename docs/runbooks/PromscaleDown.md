# PromscaleDown
## Meaning
Promscale instance is unreachable

## Impact
Monitoring data cannot be collected and as a result there is no visibility into the operations of promscale.
Promscale operations may be affected, and it may not be possible to ingest or query metrics or tracing data.

## Diagnosis
1. Check if Promscale is up and running
2. Check if data is scraped in Prometheus

## Mitigation
Ensure that Promscale metric endpoint is being scraped by Prometheus
