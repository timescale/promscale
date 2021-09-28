// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package plugin

import (
	"context"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
)

type mockReader struct{}

func newMockJaegerReader() jaeger_query.JaegerReaderPlugin {
	return mockReader{}
}

func (m mockReader) GetServices(_ context.Context) ([]string, error) {
	return []string{"demo_service_1", "demo_service_2", "demo_service_3"}, nil
}

func (m mockReader) GetOperations(_ context.Context, r storage_v1.GetOperationsRequest) (storage_v1.GetOperationsResponse, error) {
	if r.Service == "demo_service_1" && r.SpanKind == "SPAN_KIND_SERVER" {
		return storage_v1.GetOperationsResponse{
			OperationNames: []string{"demo_operation_1", "demo_operation_2"},
			Operations: []*storage_v1.Operation{
				{
					Name:     "demo_service_1",
					SpanKind: "SPAN_KIND_SERVER",
				},
				{
					Name:     "demo_service_2",
					SpanKind: "SPAN_KIND_SERVER",
				},
			},
		}, nil
	}
	return storage_v1.GetOperationsResponse{}, nil
}

func (m mockReader) GetTrace(ctx context.Context, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	return nil, nil
}

func (m mockReader) FindTraces(ctx context.Context, query *storage_v1.TraceQueryParameters) ([]*model.Trace, error) {
	return nil, nil
}

func (m mockReader) FindTraceIDs(ctx context.Context, query *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	return nil, nil
}
