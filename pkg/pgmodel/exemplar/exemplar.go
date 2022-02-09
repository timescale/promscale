// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package exemplar

import (
	"context"
	"fmt"
	pgquerier "github.com/timescale/promscale/pkg/pgmodel/querier"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

// QueryExemplar fetches the exemplars from the database using the queryable.
func QueryExemplar(ctx context.Context, query string, queryable pgquerier.Queryable, start, end time.Time) ([]model.ExemplarQueryResult, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}
	selectors := parser.ExtractSelectors(expr)
	if len(selectors) < 1 {
		// We have nothing to fetch if there are no selectors.
		return []model.ExemplarQueryResult{}, nil
	}
	querier := queryable.ExemplarsQuerier(ctx)
	results, err := querier.Select(start, end, selectors...)
	if err != nil {
		return nil, fmt.Errorf("selecting exemplars: %w", err)
	}
	return results, nil
}
