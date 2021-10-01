// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func singleTrace(ctx context.Context, conn pgxconn.PgxConn, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	traceIDstr := traceID.TraceID.String()
	fmt.Println("traceId received", traceIDstr)

	query, _ := buildQuery(queryGetTrace, nil, &traceIDstr)
	fmt.Println("single trace", query)

	spanRowsIterator, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	batch, err := getTraces(traceRowsIterator(spanRowsIterator))
	if err != nil {
		return nil, fmt.Errorf("get traces: %w", err)
	}

	traces := batchSliceToTraceSlice(batch)
	if len(traces) > 1 {
		return nil, fmt.Errorf("more than one trace received in getTrace API. Received %d traces", len(traces))
	} else if len(traces) == 0 {
		return nil, nil
	}

	return traces[0], nil
}

func findTraces(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]*model.Trace, error) {
	query, hasDuration := buildQuery(queryFindTraces, q, nil)
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
	query, hasDuration := buildQuery(queryFindTraceIds, q, nil)
	var args []interface{}
	if hasDuration {
		args = []interface{}{q.DurationMin, q.DurationMax}
	}
	traceIdsIterator, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer traceIdsIterator.Close()

	var traceId pgtype.UUID
	for traceIdsIterator.Next() {
		if err = traceIdsIterator.Scan(&traceId); err != nil {
			return nil, fmt.Errorf("scanning trace ids iterator: %w", err)
		}
	}

	return traceIds, nil
}
