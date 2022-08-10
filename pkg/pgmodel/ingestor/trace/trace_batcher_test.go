// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/tests/testdata"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

type noopWriter struct {
	callBack func(t ptrace.Traces)
}

func (nw *noopWriter) InsertTraces(ctx context.Context, traces ptrace.Traces) error {
	nw.callBack(traces)
	return nil
}

func (nw *noopWriter) Close() {}

func TestTraceBatcherBatching(t *testing.T) {
	traces := testdata.GenerateTestTraces(4)
	batchChecker := func(traces ptrace.Traces) {
		if traces.SpanCount() != 1000 { // one trace has 250 spans
			t.Errorf("wrong batch size. got: %v", traces.SpanCount())
		}
	}
	batcherConfig := BatcherConfig{
		BatchTimeout: time.Hour, // we set long enough batch timeout
		Writers:      1,
		Batchers:     1,
	}
	batcher := NewBatcher(batcherConfig, &noopWriter{callBack: batchChecker})
	batcher.Run()
	for _, t := range traces {
		batcher.in[0] <- insertTracesReq{payload: t, response: make(chan error, 1)}
	}
	batcher.Stop()
}

func TestTraceBatcherTimeout(t *testing.T) {
	traces := testdata.GenerateTestTraces(4)
	flushCounter := 0
	batchChecker := func(traces ptrace.Traces) {
		flushCounter++
		if traces.SpanCount() != 250 { // one trace has 250 spans meaning there is no batching on size
			t.Errorf("wrong batch size. got: %v", traces.SpanCount())
		}
	}
	batcher := NewBatcher(BatcherConfig{Writers: 1, Batchers: 1}, &noopWriter{callBack: batchChecker})
	batcher.Run()
	for _, t := range traces {
		batcher.in[0] <- insertTracesReq{payload: t, response: make(chan error, 1)}
		time.Sleep(350 * time.Millisecond) // to make sure batch timeout is reached
	}
	batcher.Stop()
	if flushCounter != 4 {
		t.Errorf("wrong number of batch flushes. got: %v", flushCounter)
	}
}
