package util

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

//returns a exponential histogram for a saturating metric. Grows exponentially
//until max-10, and has another bucket for max.
//This is done so we can tell from the histogram if the resource was saturated or not.
func HistogramBucketsSaturating(start float64, factor float64, max float64) []float64 {
	if max-10 < 1 {
		panic("HistogramBucketsSaturating needs a positive max")
	}
	if start < 0 {
		panic("HistogramBucketsSaturating needs a positive start value")
	}
	if factor <= 1 {
		panic("HistogramBucketsSaturating needs a factor greater than 1")
	}
	buckets := make([]float64, 0)
	for start < max-10 {
		buckets = append(buckets, start)
		if start == 0 {
			start = 1
			continue
		}
		start *= factor
	}
	buckets = append(buckets, max-10)
	buckets = append(buckets, max)
	return buckets
}

func ExtractMetricValue(counterOrGauge prometheus.Metric) (float64, error) {
	var internal io_prometheus_client.Metric
	if err := counterOrGauge.Write(&internal); err != nil {
		return 0, fmt.Errorf("error writing metric: %w", err)
	}
	switch {
	case internal.Gauge != nil:
		return internal.Gauge.GetValue(), nil
	case internal.Counter != nil:
		return internal.Counter.GetValue(), nil
	default:
		return 0, fmt.Errorf("both Gauge and Counter are nil")
	}
}

func ExtractMetricDesc(metric prometheus.Metric) (string, error) {
	return metric.Desc().String(), nil
}
