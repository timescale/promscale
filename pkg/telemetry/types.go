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
type telemetryPromQL struct {
	stat       string
	promqlExpr string
}

func (t telemetryPromQL) Query() interface{} {
	return t.promqlExpr
}

func (t telemetryPromQL) Name() string { return t.stat }
