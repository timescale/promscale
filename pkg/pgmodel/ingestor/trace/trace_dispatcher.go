// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/tracer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/trace"
	uber_atomic "go.uber.org/atomic"
)

// Dispatcher manages trace batcher and ingestion
// Dispatches user request which are combined into batches later on
type Dispatcher struct {
	batcher       *Batcher
	async         bool   // 'true' means that we don't wait for request to be persisted
	curBatcherIdx uint32 // used for round-robin
	stopped       *uber_atomic.Bool
}

func NewDispatcher(writer Writer, async bool, batchConfig BatcherConfig) *Dispatcher {
	batcher := NewBatcher(batchConfig, writer)
	batcher.Run()
	return &Dispatcher{batcher: batcher, async: async, stopped: uber_atomic.NewBool(false)}
}

type insertTracesReq struct {
	payload  ptrace.Traces
	response chan error
	spanCtx  trace.SpanContext
}

func (td *Dispatcher) InsertTraces(ctx context.Context, traces ptrace.Traces) (err error) {
	code := "200"
	defer func() {
		if err != nil {
			code = "500"
		}
		if !td.async {
			metrics.IngestorRequests.With(prometheus.Labels{"type": "trace", "code": code}).Inc()
		}
	}()
	_, span := tracer.Default().Start(ctx, "trace-dispatcher")
	defer span.End()
	if td.stopped.Load() {
		return fmt.Errorf("trace dispatcher stopped")
	}
	batcherIdx, err := td.getBatcherIdx(ctx, traces)
	if err != nil {
		return err
	}
	req := insertTracesReq{payload: traces, response: make(chan error, 1), spanCtx: span.SpanContext()}
	td.batcher.send(req, batcherIdx)
	metrics.IngestorRequestsQueued.With(prometheus.Labels{"type": "trace", "queue_idx": fmt.Sprintf("%d", batcherIdx)}).Inc()
	span.AddEvent("Trace request dispatched")
	if td.async {
		go func() {
			err = <-req.response
			span.AddEvent("Completed async trace request")
			if err != nil {
				span.AddEvent("Error while processing async request")
				log.Error("async", td.async, "error", err)
				code = "500"
			}
			metrics.IngestorRequests.With(prometheus.Labels{"type": "trace", "code": code}).Inc()
		}()
		return nil
	} else {
		select {
		case err = <-req.response:
		case <-ctx.Done():
			err = ctx.Err()
		}
		return err
	}
}

// This is to reduce lock contention for DB when spans are coming one by one (eg from Jaeger collector).
// If it's only one span we shard by it's TraceID so spans with the same TraceID end up in the same batcher.
// Otherwise we roun-robin between batchers
func (td *Dispatcher) getBatcherIdx(ctx context.Context, traces ptrace.Traces) (int, error) {
	numberOfBatchers := td.batcher.config.Batchers
	// detect if there is only one span
	if traces.ResourceSpans().Len() == 1 {
		scopeSpans := traces.ResourceSpans().At(0).ScopeSpans()
		if scopeSpans.Len() == 1 && scopeSpans.At(0).Spans().Len() == 1 {
			traceID := scopeSpans.At(0).Spans().At(0).TraceID().Bytes()
			hash := xxhash.Sum64(traceID[:])
			return int(hash % uint64(numberOfBatchers)), nil
		}
	}
	// round-robin
	next := int(atomic.AddUint32(&td.curBatcherIdx, 1))
	return next % numberOfBatchers, nil
}

func (td *Dispatcher) Close() {
	td.stopped.Store(true)
	td.batcher.Stop()
}
