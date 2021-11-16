// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
)

type telemetry interface {
	Name() string
	Query() interface{}
}

// telemetrySQL fetches telemetry information by evaluating an SQL expression.
// Only one value must be returned by the query, as telemetry information needs a single value.
type telemetrySQL struct {
	stat string
	sql  string // Should return only one output as a string.
	typ  telemetryType
}

func (t telemetrySQL) Query() interface{} {
	return t.sql
}

func (t telemetrySQL) Name() string { return t.stat }

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

// telemetryPromQL fetches telemetry information by running a PromQL expression.
// The provided expression must return exactly one sample value.
// More than one samples will result in an error, as the telemetry will require
// a single value only.
type telemetryPromQL struct {
	stat   string
	promql string
}

func (t telemetryPromQL) Query() interface{} {
	return t.promql
}

func (t telemetryPromQL) Name() string { return t.stat }
