// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
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
		AND _prom_ext.jsonb_digest(value) = _prom_ext.jsonb_digest(to_jsonb($1::text))
	)
	AND %s
`
)

func getOperations(ctx context.Context, conn pgxconn.PgxConn, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var (
		operationNames, spanKinds []string
		operationsResp            []spanstore.Operation
	)

	args := []interface{}{query.ServiceName}
	kindQual := "TRUE"
	if len(query.SpanKind) > 0 {
		pgEnum, err := spanKindStringToInternal(query.SpanKind)
		if err != nil {
			return operationsResp, fmt.Errorf("converting query enum: %w", err)
		}
		args = append(args, pgEnum)
		kindQual = "o.span_kind = $2"
	}

	sqlQuery := fmt.Sprintf(getOperationsSQLFormat, kindQual)

	if err := conn.QueryRow(ctx, sqlQuery, args...).Scan(&operationNames, &spanKinds); err != nil {
		return operationsResp, fmt.Errorf("fetching operations: %w", err)
	}

	if len(operationNames) != len(spanKinds) {
		return operationsResp, fmt.Errorf("entries not same in operation-name and span-kind")
	}

	operationsResp = make([]spanstore.Operation, len(spanKinds))

	for i := 0; i < len(operationNames); i++ {
		operationsResp[i] = internalToJaegerOperation(operationNames[i], spanKinds[i])
	}
	return operationsResp, nil
}

func textArraytoStringArr(s pgtype.FlatArray[pgtype.Text]) ([]string, error) {
	d := make([]string, len(s))
	for i, v := range s {
		if !v.Valid {
			return nil, errors.New("can't assign NULL to string")
		}
		d[i] = v.String
	}
	return d, nil
}
