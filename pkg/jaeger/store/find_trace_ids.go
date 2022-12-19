// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func findTraceIDs(ctx context.Context, builder *Builder, conn pgxconn.PgxConn, q *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	tInfo, err := FindTagInfo(ctx, q, conn)
	if err != nil {
		return nil, fmt.Errorf("querying trace tags error: %w", err)
	}
	if tInfo == nil {
		//tags cannot be matched
		return []model.TraceID{}, nil
	}
	query, params := builder.findTraceIDsQuery(q, tInfo)
	rows, err := conn.Query(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rows.Close()

	traceIds := make([]model.TraceID, 0)
	var traceIdUUID pgtype.UUID
	for rows.Next() {
		if rows.Err() != nil {
			return nil, fmt.Errorf("trace ids row iterator: %w", rows.Err())
		}
		if err = rows.Scan(&traceIdUUID); err != nil {
			return nil, fmt.Errorf("scanning trace ids: %w", err)
		}
		trace_id, err := model.TraceIDFromBytes(traceIdUUID.Bytes[:])
		if err != nil {
			return nil, fmt.Errorf("converting trace_id UUID->model: %w", err)
		}
		traceIds = append(traceIds, trace_id)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("trace ids row iterator: %w", rows.Err())
	}
	return traceIds, nil
}
