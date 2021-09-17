package jaeger_query

import (
	"context"
	"fmt"

	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getOperations = `select array_agg(sn.name), array_agg(s.span_kind::text) from
_ps_trace.span_name sn inner join _ps_trace.span s
	on sn.id = s.name_id
where _ps_trace.val_text(s.resource_tags, 'int18')=$1 and s.span_kind=$2` // change int18 => service.name

func operations(ctx context.Context, conn pgxconn.PgxConn, query storage_v1.GetOperationsRequest) (storage_v1.GetOperationsResponse, error) {
	var (
		operationNames, spanKinds []string
		operationsResp            storage_v1.GetOperationsResponse
	)
	query.Service = "3"
	query.SpanKind = "SPAN_KIND_CONSUMER"
	if err := conn.QueryRow(ctx, getOperations, query.Service, query.SpanKind).Scan(&operationNames, &spanKinds); err != nil {
		return operationsResp, fmt.Errorf("fetching operations: %w", err)
	}
	if len(operationNames) != len(spanKinds) {
		return operationsResp, fmt.Errorf("entries not same in operation-name and span-kind")
	}

	operationsResp.OperationNames = operationNames
	operations := make([]*storage_v1.Operation, len(spanKinds))

	for i := 0; i < len(operationNames); i++ {
		operations[i] = &storage_v1.Operation{
			Name:     operationNames[i],
			SpanKind: spanKinds[i],
		}
	}
	operationsResp.Operations = operations

	return operationsResp, nil
}
