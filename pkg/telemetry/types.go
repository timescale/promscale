// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

type telemetry interface {
	Sync() (interface{}, error)
}

type telemetrySQL struct {
	conn pgxconn.PgxConn
	stat string
	sql  string // Should return only one output as a string.
}

func (t telemetrySQL) Sync() (interface{}, error) {
	var value string
	if err := t.conn.QueryRow(context.Background(), t.sql).Scan(&value); err != nil {
		return nil, fmt.Errorf("stat: %s: scanning result: %w", t.stat, err)
	}
	return value, nil
}

// telemetryMetric fetches telemetry information from the underlying Prometheus metric value.
// The underlying Prometheus metric must be a Counter or a Gauge.
type telemetryMetric struct {
	stat string
	// metric should be either Gauge or a Counter.
	metric prometheus.Metric
}

func (t telemetryMetric) Sync() (interface{}, error) {
	value, err := extractMetricValue(t.metric)
	if err != nil {
		return 0, fmt.Errorf("error extracting metric value: %w", err)
	}
	return value.GetValue(), nil
}

// telemetryPromQL fetches telemetry information by running a PromQL expression.
type telemetryPromQL struct {
	stat       string
	promqlExpr string
}

func (t telemetryPromQL) Sync() (interface{}, error) {
	return nil, nil
}

type rawMetric interface {
	GetValue() float64
}

func extractMetricValue(metric prometheus.Metric) (rawMetric, error) {
	var internal io_prometheus_client.Metric
	if err := metric.Write(&internal); err != nil {
		return nil, fmt.Errorf("error writing metric: %w", err)
	}
	if !isNil(internal.Gauge) {
		return internal.Gauge, nil
	} else if !isNil(internal.Counter) {
		return internal.Counter, nil
	}
	return nil, fmt.Errorf("both Gauge and Counter are nil")
}

func isNil(v interface{}) bool {
	return v == nil
}
