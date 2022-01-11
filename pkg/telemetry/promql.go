// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/promql"
)

type promqlTelemetry struct {
	name        string
	query       string
	parsedQuery promql.Query
}

func (t *promqlTelemetry) execute(engine *promql.Engine, queryable promql.Queryable) (float64, error) {
	if t.parsedQuery == nil {
		qry, err := engine.NewInstantQuery(queryable, t.query, time.Now())
		if err != nil {
			return 0, fmt.Errorf("creating instant query: %w", err)
		}
		t.parsedQuery = qry
	}

	result := t.parsedQuery.Exec(context.Background())
	if result.Err != nil {
		return 0, fmt.Errorf("error executing promql query '%s': %w", t.query, result.Err)
	}

	value, err := getValue(result)
	if err != nil {
		return 0, fmt.Errorf("error extracting value from promql query '%s' response: %w", t.query, err)
	}
	return value, nil
}

func getValue(result *promql.Result) (float64, error) {
	vec, err := result.Vector()
	if err != nil {
		return 0, fmt.Errorf("error reading vector response: %w", err)
	}
	numSamples := len([]promql.Sample(vec))
	if numSamples > 1 {
		return 0, fmt.Errorf("evaluated result must not contain more than one sample. Got num samples: %d", numSamples)
	}

	var value float64
	if numSamples == 1 {
		value = []promql.Sample(vec)[0].V
	}

	return value, nil
}

var promqlStats = []promqlTelemetry{
	{
		name:  "promql_query_execution_time_p50",
		query: "histogram_quantile(0.5, sum by(le) (rate(promscale_metrics_query_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_execution_time_p90",
		query: "histogram_quantile(0.9, sum by(le) (rate(promscale_metrics_query_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_execution_time_p95",
		query: "histogram_quantile(0.95, sum by(le) (rate(promscale_metrics_query_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_execution_time_p99",
		query: "histogram_quantile(0.99, sum by(le) (rate(promscale_metrics_query_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_remote_read_batch_execution_time_p50",
		query: "histogram_quantile(0.5, sum by(le) (rate(promscale_metrics_query_remote_read_batch_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_remote_read_batch_execution_time_p90",
		query: "histogram_quantile(0.9, sum by(le) (rate(promscale_metrics_query_remote_read_batch_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_remote_read_batch_execution_time_p95",
		query: "histogram_quantile(0.95, sum by(le) (rate(promscale_metrics_query_remote_read_batch_duration_seconds_bucket[1h])))",
	}, {
		name:  "promql_query_remote_read_batch_execution_time_p99",
		query: "histogram_quantile(0.99, sum by(le) (rate(promscale_metrics_query_remote_read_batch_duration_seconds_bucket[1h])))",
	}, {
		name:  "trace_query_execution_time_p50",
		query: "histogram_quantile(0.5, sum by(le) (rate(promscale_trace_fetch_traces_api_execution_duration_seconds_bucket[1h])))",
	}, {
		name:  "trace_query_execution_time_p90",
		query: "histogram_quantile(0.9, sum by(le) (rate(promscale_trace_fetch_traces_api_execution_duration_seconds_bucket[1h])))",
	}, {
		name:  "trace_query_execution_time_p95",
		query: "histogram_quantile(0.95, sum by(le) (rate(promscale_trace_fetch_traces_api_execution_duration_seconds_bucket[1h])))",
	}, {
		name:  "trace_query_execution_time_p99",
		query: "histogram_quantile(0.99, sum by(le) (rate(promscale_trace_fetch_traces_api_execution_duration_seconds_bucket[1h])))",
	},
}
