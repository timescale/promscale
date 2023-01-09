// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	pgMetrics "github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/util"
)

var metrics *Metrics

type Metrics struct {
	// Using the first word in struct to ensure proper alignment in 32-bit systems.
	// Reference: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	LastRequestUnixNano       int64
	HTTPRequestDuration       *prometheus.HistogramVec
	RemoteReadReceivedQueries prometheus.Counter
}

func updateIngestMetrics(code string, duration, receivedSamples, receivedMetadata float64) {
	pgMetrics.IngestorRequests.With(prometheus.Labels{"type": "metric", "code": code}).Inc()
	pgMetrics.IngestorDuration.With(prometheus.Labels{"type": "metric", "code": code}).Observe(duration)
	pgMetrics.IngestorItemsReceived.With(prometheus.Labels{"type": "metric", "kind": "sample"}).Observe(receivedSamples)
	pgMetrics.IngestorItemsReceived.With(prometheus.Labels{"type": "metric", "kind": "metadata"}).Observe(receivedMetadata)
}

func updateQueryMetrics(handler, code, reason string, duration float64) {
	pgMetrics.Query.With(prometheus.Labels{"type": "metric", "code": code, "handler": handler, "reason": reason}).Inc()
	pgMetrics.QueryDuration.With(prometheus.Labels{"type": "metric", "code": code, "handler": handler}).Observe(duration)
}

// InitMetrics sets up and returns the Prometheus metrics which Promscale exposes.
// This needs to be set before calling objects from the api package.
func InitMetrics() *Metrics {
	if metrics != nil {
		return metrics
	}
	metrics = createMetrics()
	prometheus.MustRegister(
		metrics.HTTPRequestDuration,
		metrics.RemoteReadReceivedQueries,
	)
	return metrics
}

func createMetrics() *Metrics {
	return &Metrics{
		LastRequestUnixNano: time.Now().UnixNano(),
		HTTPRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: util.PromNamespace,
				Subsystem: "api",
				Name:      "http_request_duration_ms",
				Help:      "Duration of HTTP request in milliseconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"path"},
		),
		RemoteReadReceivedQueries: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace:   util.PromNamespace,
				Subsystem:   "query",
				Name:        "remote_read_queries_total",
				Help:        "Number of remote-read queries received.",
				ConstLabels: map[string]string{"type": "metric", "handler": "/read"},
			},
		),
	}
}
