package querier

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
)

// promqlMetadata is metadata received directly from our native PromQL engine.
type promqlMetadata struct {
	//alwyas set
	matchers []*labels.Matcher

	//following may be nil
	selectHints *storage.SelectHints
	queryHints  *QueryHints
	path        []parser.Node
}

func GetPromQLMetadata(matchers []*labels.Matcher, hints *storage.SelectHints, qh *QueryHints, path []parser.Node) *promqlMetadata {
	return &promqlMetadata{selectHints: hints, queryHints: qh, path: path, matchers: matchers}
}

type timeFilter struct {
	metric      string
	schema      string
	column      string
	seriesTable string
	start       string
	end         string
}

type evalMetadata struct {
	isSingleMetric  bool
	isExemplarQuery bool
	metric          string
	timeFilter      timeFilter
	clauses         []string
	values          []interface{}
	*promqlMetadata
	*RollupConfig
}

func GetMetadata(clauses []string, values []interface{}) *evalMetadata {
	return &evalMetadata{clauses: clauses, values: values}
}

// getEvaluationMetadata gives the metadata that will be required in evaluating a query.
func getEvaluationMetadata(tools *queryTools, start, end int64, promMetadata *promqlMetadata) (*evalMetadata, error) {
	matchers := promMetadata.matchers
	if tools.rAuth != nil {
		matchers = tools.rAuth.AppendTenantMatcher(matchers)
	}
	// Build a subquery per metric matcher.
	builder, err := BuildSubQueries(matchers)
	if err != nil {
		return nil, fmt.Errorf("build subQueries: %w", err)
	}

	metric := builder.GetMetricName()
	timeFilter := timeFilter{
		metric: metric,
		schema: builder.GetSchemaName(),
		column: builder.GetColumnName(),
		start:  toRFC3339Nano(start),
		end:    toRFC3339Nano(end),
	}

	// If all metric matchers match on a single metric (common case),
	// we query only that single metric.
	if metric != "" {
		// Single metric.
		clauses, values, err := builder.Build(false)
		if err != nil {
			return nil, fmt.Errorf("building single metric clauses: %w", err)
		}
		return &evalMetadata{
			isSingleMetric: true,
			metric:         metric,
			timeFilter:     timeFilter,
			clauses:        clauses,
			values:         values,
			promqlMetadata: promMetadata,
		}, nil
	}
	// Multiple metric.
	clauses, values, err := builder.Build(true)
	if err != nil {
		return nil, fmt.Errorf("building multiple metric clauses: %w", err)
	}
	return &evalMetadata{
		timeFilter:     timeFilter,
		clauses:        clauses,
		values:         values,
		promqlMetadata: promMetadata,
	}, nil
}
