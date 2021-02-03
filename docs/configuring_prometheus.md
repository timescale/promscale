# Configuring Prometheus for better Performance with Promscale

This document mentions details about configuring Prometheus's remote-write settings to maximize performance
from Promscale.

Prometheus is a monitoring tool, that scrapes targets and sends scraped metric data to remote-storage systems
like Promscale. The samples are pushed by Prometheus through the remote-write component of Prometheus. You
can configure the remote-write component based on the requirements and tune the component accordingly. Details
related to the remote-write configuration can be found [here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

Remote-write performance is mainly dependent on `queue_config`. Changing values in `queue_config` will allow
the remote-write component to adjust to different scenarios. For most cases, they should be:

```yaml
remote_write:
  remote_timeout: 30s
  queue_config:
    capacity: 10000
    max_samples_per_send: 3000
    batch_send_deadline: 10s
    min_shards: 4
    max_shards: 200
    min_backoff: 100ms
    max_backoff: 10s
```

Please read further for explanation of each setting.

### remote timeout (remote-write)

Set by `remote_timeout:` field in `remote_write`.

Remote-timeout corresponds to the timeout of HTTP `POST` requests, that carry the samples batch to the remote storage.
If Promscale is far from where Prometheus is deployed, or you have a high cardinality of data being ingested, you can
set this to `remote_timeout: 30s`. However, higher values for `remote_timeout` can be considered, based on the requirements.

### capacity

Set by `capacity:` field in `queue_config`.

This sets the maximum number of samples that each queues in a remote-write shards can hold. If you have
higher throughput, then the capacity of queues should be `capacity: 10000` at least.

### maximum samples per send

Set by `max_samples_per_send:` field in `queue_config`.

It denotes the maximum number of samples that can fit in a single write request to the remote storage system. Samples batch
less than `max_samples_per_send:` will be sent only when `batch_send_deadline:` expires.

### batch send deadline

Set by `batch_send_deadline:` field in `queue_config`.

Batch send deadline denotes the maximum time allowed for a samples batch to be in the queue of Prometheus's shards.
When this deadline is expired, the samples batch is sent to the remote storage, even if the `max_samples_per_send` is yet to be full. You should
set this high if you have a higher cardinality. Ideally, it should be set to `batch_send_deadline: 10s`.

### number of shards

Set by `min_shards:` field in `queue_config`.

Shards are they actual elements of remote-write component that push data to remote storage. Multiple shards send data concurrently.
Thus, the number of shards sets the amount of parallel requests sent to the remote-write endpoint. Since Promscale is
optimized for concurrent inserts, it will perform better with more shards. Ideally, you can set `min_shards: 4` & `max_shards: 200`.
Shards increase their count (till `max_shards`) if the write-endpoint is unable to keep up with the rate of samples scraped by the Prometheus instance.
However, you should also keep in mind that increasing shards by large values can impact the memory usage.

### maximum retry delay

Set by `min_backoff` & `max_backoff` fields in `queue_config`.

The remote-write component implements backoff duration on requests to write-endpoint if they fail with a recoverable error
i.e., retry the write request again after a pause. This can be particularly useful if the remote-storage is facing some rate-limiting. You can set this
to `min_backoff: 1s` and `max_backoff: 10s`. However, you can set higher values for `*_backoffs:` based on the requirements.
