// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package integration_tests

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

const (
	startTime int64 = 1577836800000
	endTime   int64 = 1578886800000
)

// This function is used to generate timeseries used for ingesting into
// Prometheus and the connector to verify same results are being returned.
func generateLargeTimeseries() (ts []prompb.TimeSeries, mint, maxt int64) {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "aaa", Value: "000"},
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3"},
				{Name: "instance", Value: "2"},
			},
		},
	}

	for i := range metrics {
		metrics[i].Samples = generateSamples(i + 1)
	}

	return metrics, startTime, endTime
}

func generateSamples(index int) []prompb.Sample {
	var (
		delta           = float64(index * 2)
		timeDelta int64 = 30000
	)
	samples := make([]prompb.Sample, 0, 3)
	i := 0
	time := startTime + (timeDelta * int64(i))

	for time < endTime {
		samples = append(samples, prompb.Sample{
			Timestamp: time,
			Value:     delta * float64(i),
		})
		i++
		time = startTime + (timeDelta * int64(i))
	}

	return samples
}

// This function is used to generate timeseries used for ingesting into
// Prometheus and the connector to verify same results are being returned.
func generateVeryLargeTimeseries() (ts []prompb.TimeSeries, mint, maxt int64) {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "aaa", Value: "000"},
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: "aaa", Value: "000"},
				{Name: labels.MetricName, Value: "metric_1_a"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1_a"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_1_a"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2_a"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2_a"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_2_a"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3_a"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: labels.MetricName, Value: "metric_3_a"},
				{Name: "instance", Value: "2"},
			},
		},
	}

	for i := range metrics {
		metrics[i].Samples = generateSamples(i + 1)
	}

	return metrics, startTime, endTime
}
