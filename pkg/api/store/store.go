package store

import (
	"context"
	"fmt"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"net/http"
)

type connector struct {
	url string
	httpClient *http.Client
}

func New() spanstore.Reader {
	return &connector{}
}

func (c *connector) GetServices(ctx context.Context) ([]string, error) {
	fmt.Println("services called")
	var services []string
	return services, nil
}

func (c *connector) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation
	return operations, nil
}

func (c *connector) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	return nil, nil
}

func (c *connector) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	return []*model.Trace{}, nil
}

func (c *connector) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	var traceIDs []model.TraceID
	return traceIDs, nil
}