// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

func NewTraceServer(i ingestor.DBInserter) ptraceotlp.Server {
	return &tracesServer{
		ingestor: i,
	}
}

type tracesServer struct {
	ingestor ingestor.DBInserter
}

func (t *tracesServer) Export(ctx context.Context, tr ptraceotlp.Request) (ptraceotlp.Response, error) {
	return ptraceotlp.NewResponse(), t.ingestor.IngestTraces(ctx, tr.Traces())
}
