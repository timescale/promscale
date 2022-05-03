// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	getOperationsSQLFormat = `
SELECT
	array_agg(o.span_name),
	array_agg(o.span_kind)
FROM
	_ps_trace.operation o
WHERE
	service_name_id = (
		SELECT id
		FROM _ps_trace.tag
		WHERE key = 'service.name'
		AND key_id = 1
		AND value = to_jsonb($1::text)
	)
	AND %s
`
)

func getOperations(ctx context.Context, conn pgxconn.PgxConn, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var (
		pgOperationNames, pgSpanKinds pgtype.TextArray
		operationsResp                []spanstore.Operation
	)

	args := []interface{}{query.ServiceName}
	kindQual := "TRUE"
	if len(query.SpanKind) > 0 {
		pgEnum, err := getPGKindEnum(query.SpanKind)
		if err != nil {
			return operationsResp, fmt.Errorf("converting query enum: %w", err)
		}
		args = append(args, pgEnum)
		kindQual = "o.span_kind = $2"
	}

	sqlQuery := fmt.Sprintf(getOperationsSQLFormat, kindQual)

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

	operationsResp = make([]spanstore.Operation, len(spanKinds))

	for i := 0; i < len(operationNames); i++ {
		operationsResp[i] = spanstore.Operation{
			Name:     operationNames[i],
			SpanKind: spanKinds[i],
		}
	}
	return operationsResp, nil
}

func textArraytoStringArr(s pgtype.TextArray) ([]string, error) {
	var d []string
	if err := s.AssignTo(&d); err != nil {
		return []string{}, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}
