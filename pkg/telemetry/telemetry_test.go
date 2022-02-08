// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package telemetry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetric(t *testing.T) {
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "test", Name: "extraction"})

	engine := &engineImpl{}
	_, has := engine.metrics.Load("some_stats")
	require.False(t, has)

	require.NoError(t, engine.RegisterMetric("some_stats", metric))

	_, has = engine.metrics.Load("some_stats")
	require.True(t, has)

	wrongMetric := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "test", Name: "wrong", Buckets: prometheus.DefBuckets})
	wrongMetric.Observe(164)

	require.Error(t, engine.RegisterMetric("some_wrong_stats", wrongMetric))

	_, has = engine.metrics.Load("some_wrong_stats")
	require.False(t, has)
}

func TestEngineStop(t *testing.T) {
	engine := &engineImpl{}
	engine.Start()
	engine.Stop()
}
