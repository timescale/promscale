package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	QueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "duration_seconds",
			Help:      "Time taken to respond to the query/query batch.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 50, 100, 250, 500, 1000, 2500},
		}, []string{"type", "handler", "code"},
	)
	Query = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "query",
			Name:      "requests_total",
			Help:      "Number of query requests to Promscale.",
		}, []string{"type", "handler", "code", "reason"},
	)
)

func init() {
	prometheus.MustRegister(
		Query,
		QueryDuration,
	)
}
