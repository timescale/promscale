// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import "github.com/prometheus/client_golang/prometheus"

type telemetry interface {
	Name() string
	Query() interface{}
}

// telemetrySQL fetches telemetry information by evaluating an SQL expression.
// Only one value must be returned by the query, as telemetry information needs a single value.
type telemetrySQL struct {
	stat string
	sql  string // Should return only one output as a string.
	typ  telemetryResultType
}

func (t telemetrySQL) Query() interface{} {
	return t.sql
}

func (t telemetrySQL) Name() string { return t.stat }

func (t telemetrySQL) ResultType() telemetryResultType { return t.typ }

// telemetryMetric fetches telemetry information from the underlying Prometheus metric value.
// The underlying Prometheus metric must be a Counter or a Gauge.
type telemetryMetric struct {
	stat string
	// metric should be either Gauge or a Counter.
	metric prometheus.Metric
}

func (t telemetryMetric) Query() interface{} {
	return t.metric
}

func (t telemetryMetric) Name() string { return t.stat }
