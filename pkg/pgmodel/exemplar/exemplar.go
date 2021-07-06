// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package exemplar

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/promql"
)

// QueryExemplar fetches the exemplars from the database using the queryable.
func QueryExemplar(ctx context.Context, query string, queryable promql.Queryable, start, end time.Time) ([]model.ExemplarQueryResult, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("PromQL parse: %w", err)
	}
	selectors := parser.ExtractSelectors(expr)
	querier := queryable.Exemplar(ctx)
	results, err := querier.Select(start, end, selectors...)
	if err != nil {
		return nil, fmt.Errorf("selecting exemplars: %w", err)
	}
	return results, nil
}
