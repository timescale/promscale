// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"context"
	"fmt"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func singleTrace(ctx context.Context, conn pgxconn.PgxConn, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	//traceIDstr := traceID.String()

	trace := new(model.Trace)
	//if err := conn.QueryRow(ctx, getTrace, traceID).Scan(trace); err != nil {
	//	return nil, fmt.Errorf("fetching a trace with %s as ID: %w", traceID.String(), err)
	//}
	// if err := batchToSingleTrace(trace, jaegerTrace); err != nil {
	// 	return nil, fmt.Errorf("batch to single trace: %w", err)
	// }

	return trace, nil
}

func findTraces(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]*model.Trace, error) {
	query, hasDuration := buildQuery(spansQuery, q)
	fmt.Println("has dur", hasDuration)
	fmt.Println("query=>\n", query)
	var args []interface{}
	if hasDuration {
		args = []interface{}{q.DurationMin, q.DurationMax}
	}
	rawTracesIterator, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rawTracesIterator.Close()

	batch, err := getTraces(traceRowsIterator(rawTracesIterator))
	if err != nil {
		return nil, fmt.Errorf("get traces: %w", err)
	}
	return batchSliceToTraceSlice(batch), nil
}

func findTraceIDs(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	traceIds := make([]model.TraceID, 0)
	// query
	//b, hasDuration := buildQuery(traceIdsQuery, q)
	//var args []interface{}
	//if hasDuration {
	//	args = []interface{}{q.DurationMin, q.DurationMax}
	//}

	return traceIds, nil
}
