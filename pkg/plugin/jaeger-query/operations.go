package jaeger_query

import (
	"context"
	"fmt"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getOperations = ""

func operations(ctx context.Context, conn pgxconn.PgxConn, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation // todo: make this make()
	if err := conn.QueryRow(ctx, getOperations).Scan(&operations); err != nil {
		return nil, fmt.Errorf("fetching services: %w", err)
	}
	return operations, nil
}
