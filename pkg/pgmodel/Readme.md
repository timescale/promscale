# Core Promscale Functionality #

This package contains the core Promscale functionality: the code to translate Read and Write requests into things the database can understand. Logically this code is divided into two mostly separate paths:

1. The Read path, which is entered through the `Reader` interface, and translates `ReadRequest`s into SQL requests the DB can understand.
2. The Write path, entered via `Ingest()`, inserts data from `WriteRequests`

The code paths for these two areas of functionality are large independent (I believe the only things shared are the schema names and the metric cache) and may be split into separate packages at some point.

**NOTE:** This document is non-normative, and is merely a snapshot of the state of the code a particular point in time. In general, code comments supersede this document in the even they disagree.

## Read Path ##

TODO (@ante do you want to write this?)

## Write Path ##

A `WriteRequest` is made out of a `[]TimeSeries` each of which contains a set of lables, each a `{string, string}`, and a set of samples, each a `{Timestamp, Value}`. Logically, the write path deals with this write request multiple stages. In Go notation, this can be thought of as follows

```go
type WriteRequest = []TimeSeries

type TimeSeries struct {
    Labels  []Label
    Samples []Sample
}

type Label {
    Key   string
    Value string
}

type Sample {
    Timestamp int64
    Value     float64
}
```

For performance reasons, the handling these requests are done by multiple, pipelined goroutines (SEDA style). In the first stage of the pipeline, which is done on a goroutine per-request:

1. The labels `{string, string}[]` in each is normalized and canonicalized into a `Labels`.
2. The timeseries are partitioned into groups based by the metric they're going to be inserted into. And forwarded to a metric-specific goroutine.
3. The first goroutine waits until all the work for this request is completed, either successfully or erroneously, and returns the result.

When we first see a data destined for a given metric, we spin up a goroutine that performs processing specific to that metric. Whenever this goroutine sees a new insert request:

1. Replaces all the `Labels` it can with `SeriesId`s from its cache.
2. Batches the insert with other inserts going to the same metric.
3. Before forwarding the batch to the next stage, replaces all `Labels` with `SeriesIds` fetches missing `SeriesIds` from the DB.

The purpose of this metric layer is to allow for greater batching series-id fetches, and metric table `INSERT`s both of which perform notably better if they work on larger amounts of data.

After a batch is finished by the metric layer its forwarded to an arbitrary physical inserter, which translates the insert request into the actual SQL doing the insert, sends the request to the DB, and reports the result back to the original goroutine. We maintain a fixed number of physical inserters based on the number of connections to the DB we possess.

Diagrammatically the write path cn be visualized as follows:
```
|, /, \ are go channels
= is for network connections
- is for normal function calls

                  new req
                     v
                 +--------+        +-------------+
                 | Ingest | <----> | Label Cache |
                 +--------+        +-------------+
                  /     \
                 /       \
 +----------------+     +----------------+       +-----------------+
 | metric handler | ... | metric handler | <===> | DB (series IDs) |
 +----------------+     +----------------+       +-----------------+
                 \       /
                  \     /
                   \   /
                    \ /
 +----------------+  |  +----------------+       +------------------+
 | insert handler | ... | insert handler | ====> | DB (metric data) |
 +----------------+     +----------------+       +------------------+
```

`TODO` metric table creation
