// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package common

import (
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	StartTime    int64 = 1577836800000 // 01/01/2020, 05:30:00
	EndTimeLarge int64 = 1587886800000 // 26/04/2020, 13:10:00. Generates 335000/2 samples (GenerateSamples).
)

func GenerateSamples(index int) []prompb.Sample {
	var (
		delta           = float64(index * 2)
		timeDelta int64 = 60000
	)
	samples := make([]prompb.Sample, 0, 3)
	i := int64(0)
	time := StartTime + (timeDelta * i)

	for time < EndTimeLarge {
		samples = append(samples, prompb.Sample{
			Timestamp: time,
			Value:     delta * float64(i),
		})
		i++
		time = StartTime + (timeDelta * i)
	}

	return samples
}

func GeneratePromLikeLargeTimeseries() []prompb.TimeSeries {
	metrics := []prompb.TimeSeries{
		// Gauges.
		// one_gauge_metric has multiple series, which can be utilized for agg queries.
		// two_gauge_metric & three_gauge_metric has one series, hence it can be used in conjunction with one_gauge_metric
		//		to perform across metric aggs.
		{
			Labels: []prompb.Label{
				{Name: "job", Value: "benchmark"},
				{Name: model.MetricNameLabelName, Value: "one_gauge_metric"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: "job", Value: "benchmark"},
				{Name: model.MetricNameLabelName, Value: "one_gauge_metric"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "11"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "two_gauge_metric"},
				{Name: "foo", Value: "bar"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "three_gauge_metric"},
				{Name: "foo", Value: "bar"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
			},
		},
		// Counters.
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_counter_total"},
				{Name: "version", Value: "1.15"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "two_counter_total"},
				{Name: "version", Value: "1.17"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "1"},
			},
		},
		// Summaries.
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
				{Name: "quantile", Value: "0"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
				{Name: "quantile", Value: "0.25"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
				{Name: "quantile", Value: "0.5"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
				{Name: "quantile", Value: "0.75"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
				{Name: "quantile", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary_sum"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_summary_count"},
				{Name: "job", Value: "benchmark"},
				{Name: "instance", Value: "3"},
			},
		},
		// Histograms.
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_bucket"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
				{Name: "le", Value: "0.1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_bucket"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
				{Name: "le", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_bucket"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
				{Name: "le", Value: "10"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_bucket"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
				{Name: "le", Value: "100"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_sum"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "one_histogram_count"},
				{Name: "instance", Value: "2"},
				{Name: "job", Value: "benchmark"},
			},
		},
	}

	for i := range metrics {
		metrics[i].Samples = GenerateSamples(i + 1)
	}

	return metrics
}
