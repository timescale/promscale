// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/tracer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/trace"
	_ "go.uber.org/automaxprocs"
)

const (
	DefaultBatchSize    = 1000                   // this is soft limit as we might produce bigger batches in some cases
	DefaultBatchTimeout = 100 * time.Millisecond // we should aways aim at reaching size limits, not timeout
)

var (
	DefaultBatchWorkers = runtime.GOMAXPROCS(0) // package go.uber.org/automaxprocs updates this value on start
)

// Batch individual insertTracesReq.
type Batch struct {
	traces     ptrace.Traces
	spanCount  int
	reqStatus  []chan error
	ctx        context.Context
	maxSize    int
	addCounter int // counting number of insertTracesReq added
}

func NewBatch(maxBatchSize int) *Batch {
	return &Batch{
		traces:    ptrace.NewTraces(),
		spanCount: 0,
		reqStatus: make([]chan error, 0, maxBatchSize),
		ctx:       context.Background(),
		maxSize:   maxBatchSize, // this is not hard limit
	}
}

func (tb *Batch) add(in insertTracesReq) {
	_, addReqSpan := tracer.Default().Start(tb.ctx, "add-trace-req-to-batch",
		trace.WithLinks(trace.Link{SpanContext: in.spanCtx}))
	defer addReqSpan.End()
	inSpans := in.payload.SpanCount()
	if inSpans == 0 {
		return
	}
	in.payload.ResourceSpans().MoveAndAppendTo(tb.traces.ResourceSpans())
	tb.spanCount += inSpans
	tb.reqStatus = append(tb.reqStatus, in.response)
	tb.addCounter++
}

func (tb *Batch) isFull() bool {
	return tb.spanCount >= tb.maxSize
}

func (tb *Batch) isEmpty() bool {
	return tb.spanCount == 0
}

// Batcher batches trace requests and sends batches to batch writer.
// This is done to achieve better ingest performance especially b/c
// Jaeger collector sends traces one by one.
type Batcher struct {
	in              []chan insertTracesReq
	stop            chan struct{}
	batchWriter     *batchWriter
	once            sync.Once
	bufferedBatches chan Batch
	wg              sync.WaitGroup
	config          BatcherConfig
}

type BatcherConfig struct {
	Batchers     int
	Writers      int
	MaxBatchSize int
	BatchTimeout time.Duration
}

func NewBatcher(config BatcherConfig, writer Writer) *Batcher {
	validateConfig(&config)
	bufferedBatches := make(chan Batch, config.Writers*2) // we want some buffer to avoid writer waiting on batcher
	inChs := make([]chan insertTracesReq, config.Batchers)
	for i := range inChs {
		inChs[i] = make(chan insertTracesReq, config.MaxBatchSize*3) // buffer for incoming requests, especially important for async acks
	}
	return &Batcher{
		batchWriter:     newBatchWriter(config.Writers, writer, bufferedBatches),
		in:              inChs,
		stop:            make(chan struct{}),
		bufferedBatches: bufferedBatches,
		config:          config,
	}
}

func (b *Batcher) send(req insertTracesReq, batcherIdx int) {
	b.in[batcherIdx] <- req
}

func validateConfig(config *BatcherConfig) {
	if config.Batchers == 0 {
		config.Batchers = DefaultBatchWorkers
	}
	if config.MaxBatchSize == 0 {
		config.MaxBatchSize = DefaultBatchSize
	}
	if config.BatchTimeout == 0 {
		config.BatchTimeout = DefaultBatchTimeout
	}
	if config.Batchers < 1 || config.Writers < 1 {
		panic("number of batchers and writeres must be greater then zero")
	}
}

func (b *Batcher) Run() {
	for i := 0; i < b.config.Batchers; i++ {
		b.wg.Add(1)
		go func(idx int) {
			defer b.wg.Done()
			b.batch(idx)
		}(i)
	}
	b.batchWriter.run()
}

func (b *Batcher) batch(batchIdx int) {
	_, batcherSpan := tracer.Default().Start(context.Background(), "create-batches")
	ticker := time.NewTicker(b.config.BatchTimeout)
	defer ticker.Stop()
	batch := NewBatch(b.config.MaxBatchSize)
	batcherSpan.AddEvent("New Batch")
	flushBatch := func(batch *Batch) *Batch {
		b.bufferedBatches <- *batch
		batcherSpan.AddEvent("Batch sent to buffer")
		batcherSpan.AddEvent("New Batch")
		metrics.IngestorPendingBatches.With(prometheus.Labels{"type": "trace"}).Inc()
		ticker.Reset(time.Hour) // we don't want ticker firing until we get a new request so resetting it to relatively high value
		return NewBatch(b.config.MaxBatchSize)
	}
	processReq := func(req insertTracesReq) {
		metrics.IngestorRequestsQueued.With(prometheus.Labels{"type": "trace", "queue_idx": fmt.Sprintf("%d", batchIdx)}).Dec()
		batch.add(req)
		if batch.addCounter == 1 {
			// we reset timeout once we add first request into the batch
			ticker.Reset(b.config.BatchTimeout)
		}
		if batch.isFull() {
			metrics.IngestorBatchFlushTotal.With(prometheus.Labels{"type": "trace", "subsystem": "batcher", "reason": "size"}).Inc()
			batch = flushBatch(batch)
		}
	}
	for {
		select {
		case item := <-b.in[batchIdx]:
			processReq(item)
		case <-ticker.C:
			batcherSpan.AddEvent("Batch timeout reached")
			metrics.IngestorBatchFlushTotal.With(prometheus.Labels{"type": "trace", "subsystem": "batcher", "reason": "timeout"}).Inc()
			if !batch.isEmpty() {
				batch = flushBatch(batch)
			}
		case <-b.stop:
			timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
			defer cancel()
			close(b.in[batchIdx])
			// shutting down. let's drain the requests
		loop:
			for {
				select {
				case item, ok := <-b.in[batchIdx]:
					if !ok {
						break loop
					}
					processReq(item)
				case <-timeoutCtx.Done():

					log.Warn("msg", "Forced batcher shutdown due to timeout")
					if len(b.in[batchIdx]) > 0 {
						log.Warn("msg", "Some requests might not be persisted.", "batcher", batchIdx, "not_persisted_req_count", len(b.in[batchIdx]))
					}
					break loop
				}
			}
			if !batch.isEmpty() {
				flushBatch(batch)
			}
			return
		}
	}
}

func (b *Batcher) Stop() {
	b.once.Do(func() {
		close(b.stop)
		b.wg.Wait()
		close(b.bufferedBatches)
		b.batchWriter.stop()
	})
}

// batchWriter writes batches using a writer.
type batchWriter struct {
	batches    chan Batch
	numWriters int
	writer     Writer
	stopCh     chan struct{}
	once       sync.Once
	wg         sync.WaitGroup
}

func newBatchWriter(writers int, writer Writer, batches chan Batch) *batchWriter {
	return &batchWriter{
		writer:     writer,
		numWriters: writers,
		batches:    batches,
		stopCh:     make(chan struct{}),
	}
}

func (bw *batchWriter) run() {
	for i := 0; i < bw.numWriters; i++ {
		bw.wg.Add(1)
		go func() {
			defer bw.wg.Done()
			for {
				select {
				case b, ok := <-bw.batches:
					if !ok {
						return
					}
					metrics.IngestorPendingBatches.With(prometheus.Labels{"type": "trace"}).Dec()
					bw.flush(b)
				case <-bw.stopCh:
					timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*30)
					defer cancel()
					bw.drainBuffer(timeoutCtx)
					return
				}
			}
		}()
	}
}

func (bw *batchWriter) flush(b Batch) {
	_, flushSpan := tracer.Default().Start(b.ctx, "flush-batch")
	defer flushSpan.End()
	err := bw.writer.InsertTraces(context.Background(), b.traces)
	for _, req := range b.reqStatus {
		req <- err
	}

}

func (bw *batchWriter) drainBuffer(ctx context.Context) {
	for {
		select {
		case b, ok := <-bw.batches:
			if !ok {
				return
			}
			bw.flush(b)
		case <-ctx.Done():
			log.Warn("msg", "Forced batchWriter shutdown due to timeout")
			if len(bw.batches) > 0 {
				log.Warn("msg", "Some batches might not be persisted")
			}
			return
		}
	}
}

func (bw *batchWriter) stop() {
	bw.once.Do(func() {
		close(bw.stopCh)
		bw.wg.Wait()
	})
}
