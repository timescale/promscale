// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

//note the key='service.name' is there only for constraint exclusion of partitions
const getDependenciesSQL = `
SELECT
   (SELECT value #>> '{}' FROM _ps_trace.tag WHERE id = parent_op.service_name_id AND key='service.name') as parent_service,
   (SELECT value #>> '{}' FROM _ps_trace.tag WHERE id = child_op.service_name_id AND key='service.name') as child_service,
   sum(ops.cnt) as cnt
FROM ps_trace.operation_calls($1, $2) ops
INNER JOIN _ps_trace.operation child_op ON (ops.child_operation_id = child_op.id)
INNER JOIN _ps_trace.operation parent_op ON (ops.parent_operation_id = parent_op.id)
WHERE parent_op.service_name_id != child_op.service_name_id
GROUP BY parent_op.service_name_id,child_op.service_name_id`

// getDependencies returns the inter service dependencies along with a count of how many times the parent service called the child service.
func getDependencies(ctx context.Context, conn pgxconn.PgxConn, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	startTs := endTs.Add(-1 * lookback)

	var (
		parent_service string
		child_service  string
		cnt            uint64
	)

	rows, err := conn.Query(ctx, getDependenciesSQL, startTs, endTs)
	if err != nil {
		return nil, fmt.Errorf("fetching dependencies: %w", err)
	}
	defer rows.Close()

	links := make([]model.DependencyLink, 0)
	for rows.Next() {
		if err := rows.Scan(&parent_service, &child_service, &cnt); err != nil {
			return nil, fmt.Errorf("fetching dependencies: %w", err)
		}
		links = append(links, model.DependencyLink{
			Parent:    parent_service,
			Child:     child_service,
			CallCount: cnt,
			//Source is left as default
		})
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("fetching dependencies: %w", rows.Err())
	}
	return links, nil
}
