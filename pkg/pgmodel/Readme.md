# Core Promscale Functionality

This package contains the core Promscale functionality: the code to translate Read and Write requests into things the database can understand. Logically this code is divided into two mostly separate paths:

1. The Read path, which is entered through the `Reader` interface, and translates `ReadRequest`s into SQL requests the DB can understand.
2. The Write path, entered via `ingestor.IngestMetrics()`, inserts data from `WriteRequests`

The code paths for these two areas of functionality are large independent (I believe the only things shared are the schema names and the metric cache) and may be split into separate packages at some point.

**NOTE:** This document is non-normative, and is merely a snapshot of the state of the code a particular point in time. In general, code comments supersede this document in the even they disagree.

## Read Path

TODO (@ante do you want to write this?)

## Write Path

The write path accepts write requests on the Prometheus-compatible remote write endpoint. For performance reasons, the handling of these requests are done by multiple pipelined goroutines in three stages: dispatcher, metric_batcher, and copier.

Diagrammatically, the write path can be visualized as follows:

```
|, /, \ are go channels
= is for network connections
- is for normal function calls


                        new req
                           v
                    +-------------+       +--------------+
                    | dispatcher  | <---- | Series Cache |
                    +-------------+       +--------------+
                        /     \
                       /       \
       +----------------+     +----------------+       +-------------------+
       | metric_batcher | ... | metric_batcher | <===> | DB (Metric table) |
       +----------------+     +----------------+       +-------------------+
                       \       /
                        \     /                  
                         \   /                   
                          \ /                   
       +----------------+  |  +----------------+
       |     copier     | ... |     copier     |
       +----------------+     +----------------+ 
                                       |
                                       |         +------------------------+
                                       |  <===>  | DB (Series, Label IDs) |
                                       |         +------------------------+
                                       |         +--------------------------------+
                                       |  <--->  | Series, Inverted Labels Caches |
                                       |         +--------------------------------+ 
                                       |         +------------------+
                                       |  ====>  | DB (metric data) |
                                                 +------------------+
```                                                       

### The WriteRequest

A Prometheus write request is represented by the `prompb.WriteRequest` struct, which contains a `[]TimeSeries` and a `[]MetricMetadata`. Each `Timeseries` contains a set of labels, each a `{string, string}`, and a set of samples, each a `{Timestamp, Value}`. `MetricMetadata` is mentioned for completeness, but we will ignore its presence in the rest of this document. In Go notation, these objects can be thought of as follows:

```go
type WriteRequest struct {
    Timeseries []TimeSeries
    Metadata   []MetricMetadata
}

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

### Dispatcher

Internally, the dispatcher consists of two stages: ingestor, and dispatcher.

#### Ingestor

The ingestor accepts a `WriteRequest` at `ingestor.IngestMetrics(..., *WriteRequest)`. The ingestor determines if it needs to ingest `TimeSeries` only, or also `MetricMetadata` (which we were ignoring).

To process a `TimeSeries`, the ingestor:   
1. Transforms the Labels field (a `{string, string}[]`) into a `model.Series`, (which it gets from the series cache).
2. Groups the `model.Series` together with its `[]Samples` as a `model.Insertable`.
3. Puts all `model.Insertable` entries into a map, keyed by the metric name (`map[string][]model.Insertable`).

The ingestor passes this map on to the dispatcher (`dispatcher.InsertTs(...)`).

#### Dispatcher

The dispatcher processes this map by iterating through the `(key, value)` pairs (which are metric name, and the insertable (`model.Insertable`)), and sends it on a channel to the one metric batcher goroutine responsible for batching samples for that metric. If the metric batcher does not exist yet, it is created on demand and lives forever.

If asyncAcks=true, the dispatcher returns immediately, otherwise it waits for the metric batcher goroutine to return a result, and returns that in response.

### Metric batcher

The second stage consists of the metric batcher goroutines, one for each metric name. These goroutines are created in `dispatcher.InsertTs`.

On the first new insert request, the metric batcher creates a metric table in the database, if it doesn't exist yet.

From then onwards, when the metric batcher receives a new insert request on its channel it: 
1. Creates an unbuffered data-transfer channel for this batcher.
2. Initiates a read request to the copier on the copier read-request channel, sending a handle to the batcher's data-transfer channel.
3. Batches as much data as possible while waiting for a free copier. As soon as a copier frees up, it flushes the batch (even if not full). This is implemented as a loop over `select`, which either:
   1. accepts data for the metric (from the dispatcher) on its channel, writing them into a `model.Batch`, until its maximum batch size is reached (`metrics.FlushSize`, currently 2000).
   2. attempts to write the data it has batched (as an `ingestor.copyRequest`) into the unbuffered data-transfer channel to the copier.
4. When the write into the unbuffered data-transfer channel succeeds, it begins a new batch, going back to step 2.

### Copier

The final stage consists of a fixed number of goroutines, by default equal to the number of database CPU cores. The copiers are started by the dispatcher, and each hold a handle to the read-request channel, which the batcher writes requests into.

The copier:

1. Takes a mutex to be the only copier reading from the read-request channel. 
2. Reads up to `metrics.MaxInsertStmtPerTxn` (currently 100) read requests from its handle to the read-request channel, timing out after 20 milliseconds.
3. Reads all of the `ingestor.copyRequest` from all of the read requests' data-transfer channels into its own batch (note: each `copyRequest` must be for a different metric).
4. Sorts the batch of `copyRequest`s by metric table name.
5. Fills the label and series IDs in for each series in the batch.
   1. These are populated from the inverted labels and series caches, if present.
   2. Otherwise the label and series are persisted to the database, and an ID fetched and put into the inverted label and series caches.
6. Inserts the data samples for all series in its batch.
7. For each `copyRequest`, report back the success or failure to the dispatcher which the copyRequest originated from.
8. Returns to step 1.
