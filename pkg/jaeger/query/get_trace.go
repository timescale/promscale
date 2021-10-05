// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func getTrace(ctx context.Context, conn pgxconn.PgxConn, traceID model.TraceID) (*model.Trace, error) {
	query, params, err := getTraceQuery(traceID)
	if err != nil {
		return nil, fmt.Errorf("get trace query: %w", err)
	}
	rows, err := conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rows.Close()
	traces, err := scanTraces(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning traces: %w", err)
	}

	switch len(traces) {
	case 0:
		return nil, spanstore.ErrTraceNotFound
	case 1:
		return traces[0], nil
	default:
		return nil, fmt.Errorf("found more than one trace (count=%d) when searching for a traceID", len(traces))
	}

}
