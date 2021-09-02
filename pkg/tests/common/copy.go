// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package common

import "github.com/timescale/promscale/pkg/prompb"

// deep copy the metrics since we mutate them, and don't want to invalidate the tests
// todo: delete the similar functions.
func CopyMetrics(metrics []prompb.TimeSeries) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, len(metrics))
	copy(out, metrics)
	for i := range out {
		samples := make([]prompb.Sample, len(out[i].Samples))
		labels := make([]prompb.Label, len(out[i].Labels))
		copy(samples, out[i].Samples)
		copy(labels, out[i].Labels)
		out[i].Samples = samples
		out[i].Labels = labels
	}
	return out
}

// todo: delete the similar functions.
func CopyMetadata(metadata []prompb.MetricMetadata) []prompb.MetricMetadata {
	out := make([]prompb.MetricMetadata, len(metadata))
	copy(out, metadata)
	return out
}
