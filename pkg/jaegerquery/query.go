package jaegerquery

import (
	"context"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/log"
)

type Query struct {
}

func New() *Query {
	return &Query{}
}

func (p *Query) SpanReader() spanstore.Reader {
	return p
}

func (p *Query) DependencyReader() dependencystore.Reader {
	return p
}

func (p *Query) SpanWriter() spanstore.Writer {
	panic("Use Promscale + OTEL-collector to ingest traces")
}

func (p *Query) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	//query db using pgx here
	log.Warn("Get trace")
	return nil, nil
}

func (p *Query) GetServices(ctx context.Context) ([]string, error) {
	//query db using pgx here
	log.Warn("Get Svc")

	return []string{"test"}, nil
}

func (p *Query) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	//query db using pgx here
	log.Warn("Get op")

	return []spanstore.Operation{
		{Name: "testOp", SpanKind: "client"},
	}, nil
}

func (p *Query) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	//query db using pgx here
	log.Warn("find traces")
	return nil, nil
}

func (p *Query) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	//query db using pgx here
	log.Warn("find trace ids")
	return nil, nil
}

func (p *Query) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	//query db using pgx here
	log.Warn("getDependencies")
	return nil, nil
}
