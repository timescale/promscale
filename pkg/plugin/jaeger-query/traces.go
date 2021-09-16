package jaeger_query

import (
	"context"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	getTrace = "select 1"
)

func singleTrace(ctx context.Context, conn pgxconn.PgxConn, traceID model.TraceID) (*model.Trace, error) {
	trace := new(model.Trace)
	if err := conn.QueryRow(ctx, getTrace, traceID).Scan(trace); err != nil {
		return nil, fmt.Errorf("fetching a trace with %s as ID: %w", traceID.String(), err)
	}
	return trace, nil
}
