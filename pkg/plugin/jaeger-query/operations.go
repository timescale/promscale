// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	getOperations = `SELECT array_agg(sn.name),
       array_agg(s.span_kind)
FROM   _ps_trace.span_name sn
       inner join _ps_trace.span s
               ON sn.id = s.name_id
WHERE  ps_trace.val_text(s.resource_tags, 'service.name') = $1`
	spanKindSQL = ` AND s.span_kind = $2`
)

func operations(ctx context.Context, conn pgxconn.PgxConn, query storage_v1.GetOperationsRequest) (storage_v1.GetOperationsResponse, error) {
	var (
		pgOperationNames, pgSpanKinds pgtype.TextArray
		operationsResp                storage_v1.GetOperationsResponse
	)
	sqlQuery := getOperations
	// temporary, till the test dataset is being used. It makes m to "m", hence sql result is empty.
	query.Service = query.Service[1 : len(query.Service)-1]
	args := []interface{}{query.Service}
	if len(query.SpanKind) > 0 {
		sqlQuery = sqlQuery + spanKindSQL
		args = append(args, query.SpanKind)
	}
	if err := conn.QueryRow(ctx, sqlQuery, args...).Scan(&pgOperationNames, &pgSpanKinds); err != nil {
		return operationsResp, fmt.Errorf("fetching operations: %w", err)
	}

	operationNames, err := textArraytoStringArr(pgOperationNames)
	if err != nil {
		return operationsResp, fmt.Errorf("operation names: text-array-to-string-array: %w", err)
	}
	spanKinds, err := textArraytoStringArr(pgOperationNames)
	if err != nil {
		return operationsResp, fmt.Errorf("span kinds: text-array-to-string-array: %w", err)
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

func textArraytoStringArr(s pgtype.TextArray) ([]string, error) {
	var d []string
	if err := s.AssignTo(&d); err != nil {
		return []string{}, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}
