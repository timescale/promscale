// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/telemetry"
	"github.com/timescale/promscale/pkg/util"
)

var (
	traceRequestsExec = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "trace",
		Name:      "query_requests_executed_total",
		Help:      "Total number of query requests successfully executed by /getTrace and /fetchTraces API.",
	})
	// Even though this is handled by promscale_query_requests_total{subsystem="trace", handler="get_dependencies", code="200"}
	// yet we will have to keep this metric for telemetry as extracting the underlying series from a metric will require
	// changing telemetry arch that tracks the all prometheus metrics, just for this metric, which is not worth.
	dependencyRequestsExec = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: util.PromNamespace,
		Subsystem: "trace",
		Name:      "dependency_requests_executed_total",
		Help:      "Total number of dependency requests successfully executed.",
	})
)

func registerMetricsForTelemetry(t telemetry.Engine) error {
	var err error
	if err = t.RegisterMetric("promscale_trace_query_requests_executed_total", traceRequestsExec); err != nil {
		return fmt.Errorf("register 'promscale_trace_query_requests_executed_total' metric for telemetry: %w", err)
	}
	if err = t.RegisterMetric("promscale_trace_dependency_requests_executed_total", dependencyRequestsExec); err != nil {
		return fmt.Errorf("register 'promscale_trace_dependency_requests_executed_total' metric for telemetry: %w", err)
	}
	return nil
}

func init() {
	prometheus.MustRegister(
		traceRequestsExec,
		dependencyRequestsExec,
	)
}
