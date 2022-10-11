# PromscaleIngestHighDataDuplication

## Meaning

Client payload has either duplicates or retrying many a time for the
data which has been already ingested.

## Impact

Poor ingestion throughput.

## Diagnosis

1. Check Prometheus log for timeout or batch retry errors. If you are seeing
more of these errors, follow our [resource recommendations](#resource-recommendations) section for more details.

2. If you are running Prometheus in HA mode, follow [Prometheus high availability](#prometheus-high-availability) to ensure the configurations are indeed right.

## Mitigation

### Resource recommendations

Right resource allocation for Promscale and TimescaleDB would help to attain
better ingestion throughput. Follow the guideline on [resource recommendations](https://docs.timescale.com/promscale/latest/recommendations/resource-recomm/#metrics).

### Prometheus high availability

This could happen if the Prometheus HA deployment is not configured to
decorate the samples with the metadata from the replica that's pushing
the data. In this scenario, two or more Prometheus replicas from the same
cluster will be sending the exact same datapoints, and since there's no
cluster/replica metadata, Promscale doesn't have the information needed
to just accept the data from one of them and will try to persist them all.
Follow the guideline on running [Prometheus in HA mode](https://docs.timescale.com/promscale/latest/scale-ha/high-availability/#promscale-and-prometheus-high-availability) to fix the problem.
