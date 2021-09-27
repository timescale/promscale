// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"context"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type JaegerQueryReader struct {
	conn pgxconn.PgxConn
}

func NewReader(conn pgxconn.PgxConn) *JaegerQueryReader {
	return &JaegerQueryReader{
		conn: conn,
	}
}

func (r *JaegerQueryReader) GetServices(ctx context.Context) ([]string, error) {
	return services(ctx, r.conn)
}

func (r *JaegerQueryReader) GetOperations(ctx context.Context, query storage_v1.GetOperationsRequest) (storage_v1.GetOperationsResponse, error) {
	return operations(ctx, r.conn, query)
}

func (r *JaegerQueryReader) GetTrace(ctx context.Context, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	return singleTrace(ctx, r.conn, traceID)
}

func (r *JaegerQueryReader) FindTraces(ctx context.Context, query *storage_v1.TraceQueryParameters) ([]*model.Batch, error) {
	return findTraces(ctx, r.conn, query)
}

func (r *JaegerQueryReader) FindTraceIDs(ctx context.Context, query *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	return findTraceIDs(ctx, r.conn, query)
}
