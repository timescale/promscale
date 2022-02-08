// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var metrics *Metrics

type Metrics struct {
	// Using the first word in struct to ensure proper alignment in 32-bit systems.
	// Reference: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	LastRequestUnixNano int64
	InvalidReadReqs     prometheus.Counter
	InvalidWriteReqs    prometheus.Counter
	InvalidQueryReqs    prometheus.Counter
	HTTPRequestDuration *prometheus.HistogramVec
}

// InitMetrics sets up and returns the Prometheus metrics which Promscale exposes.
// This needs to be set before calling objects from the api package.
func InitMetrics() *Metrics {
	if metrics != nil {
		return metrics
	}
	metrics = createMetrics()
	prometheus.MustRegister(
		metrics.InvalidReadReqs,
		metrics.InvalidWriteReqs,
		metrics.HTTPRequestDuration,
	)

	return metrics
}

func createMetrics() *Metrics {
	return &Metrics{
		LastRequestUnixNano: time.Now().UnixNano(),
		InvalidReadReqs: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Subsystem: "api",
				Name:      "invalid_read_requests",
				Help:      "Total number of remote read requests with invalid metadata.",
			},
		),
		InvalidWriteReqs: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Subsystem: "api",
				Name:      "invalid_write_requests",
				Help:      "Total number of remote write requests with invalid metadata.",
			},
		),
		InvalidQueryReqs: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: util.PromNamespace,
				Name:      "invalid_query_requests",
				Help:      "Total number of invalid query requests with invalid metadata.",
			},
		),
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
	}
}
