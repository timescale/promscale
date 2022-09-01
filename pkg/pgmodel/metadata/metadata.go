// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package metadata

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

// MetricQuery returns metadata corresponding to metric or metric_family.
func MetricQuery(ctx context.Context, conn pgxconn.PgxConn, metric string, limit int) (map[string][]model.Metadata, error) {
	var (
		rows pgxconn.PgxRows
		err  error
	)
	if metric != "" {
		rows, err = conn.Query(ctx, "SELECT * from prom_api.get_metric_metadata($1)", metric)
	} else {
		rows, err = conn.Query(ctx, "SELECT metric_family, type, unit, help from _prom_catalog.metadata ORDER BY metric_family, last_seen DESC")
	}
	if err != nil {
		return nil, fmt.Errorf("query metric metadata: %w", err)
	}
	defer rows.Close()
	metricFamilies := make(map[string][]model.Metadata)
	for rows.Next() {
		if limit != 0 && len(metricFamilies) >= limit {
			// Limit is applied on number of metric_families and not on number of metadata.
			break
		}
		var metricFamily, typ, unit, help string
		if err := rows.Scan(&metricFamily, &typ, &unit, &help); err != nil {
			return nil, fmt.Errorf("query result: %w", err)
		}
		metricFamilies[metricFamily] = append(metricFamilies[metricFamily], model.Metadata{
			Unit: unit,
			Type: typ,
			Help: help,
		})
	}
	return metricFamilies, nil
}
