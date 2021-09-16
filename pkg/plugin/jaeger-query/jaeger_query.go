package jaeger_query

import (
	"context"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type jaegerQueryReader struct {
	conn pgxconn.PgxConn
}

func NewReader(conn pgxconn.PgxConn) spanstore.Reader {
	return &jaegerQueryReader{
		conn: conn,
	}
}

func (r *jaegerQueryReader) GetServices(ctx context.Context) ([]string, error) {
	return services(ctx, r.conn)
}

func (r *jaegerQueryReader) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	return operations(ctx, r.conn, query)
}

func (r *jaegerQueryReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	return singleTrace(ctx, r.conn, traceID)
}
