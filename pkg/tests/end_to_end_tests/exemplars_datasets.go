// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"math/rand"
	"time"

	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	startTime int64 = 1577836800000
	endTime   int64 = 1577886800000

	samplesStartTime = 1597055698698
	samplesEndTime   = 1597059548699
)

var (
	startTimeRecent = timestamp.FromTime(time.Now())
	endTimeRecent   = startTimeRecent + time.Minute.Milliseconds()*2
)

func generateRecentLargeTimeseries() []prompb.TimeSeries {
	metrics := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "aaa", Value: "000"},
				{Name: model.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_3"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_3"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "METRIC_4"},
				{Name: "foo", Value: "bar"},
			},
		},
	}

	for i := range metrics {
		metrics[i].Samples = generateRecentSamples(i + 1)
	}

	return metrics
}

func generateRecentSamples(index int) []prompb.Sample {
	var (
		delta           = float64(index * 2)
		timeDelta int64 = 30000
	)
	samples := make([]prompb.Sample, 0, 3)
	i := 0
	time := startTimeRecent + (timeDelta * int64(i))

	for time < endTimeRecent {
		samples = append(samples, prompb.Sample{
			Timestamp: time,
			Value:     delta * float64(i),
		})
		i++
		time = startTimeRecent + (timeDelta * int64(i))
	}

	return samples
}

var exemplarLabels = []prompb.Label{{Name: "TraceID", Value: "some_trace_id"}, {Name: "component", Value: "some_component"}, {Name: "job", Value: "some_job"}, {Name: "label_extra", Value: "X"}}

// getLabel returns a random label from the supplied list of labels.
// Labels in an l-set must be unique. Hence, withoutIndex ensures that
// the given index is never returned.
func getLabel(from []prompb.Label) prompb.Label {
	if len(from) == 0 {
		panic("from must not be empty")
	}
	r := rand.Int31n(int32(len(from)))
	return from[r]
}

func getNLabelsUnique(n int) []prompb.Label {
	if n == 0 {
		return []prompb.Label{}
	}
	uniqueMap := make(map[string]struct{}, n)
	i := 0
	labels := make([]prompb.Label, 0, n)
	for i < n {
		label := getLabel(exemplarLabels)
		if _, exists := uniqueMap[label.Name]; !exists {
			i++
			labels = append(labels, label)
			uniqueMap[label.Name] = struct{}{}
		}
	}
	return labels
}

func randomLabelsCount(min, max int) []prompb.Label {
	max++ // rand excludes the upper boundary. Hence, increment by one to respect max.
	r := rand.Intn(max-min) + min
	return getNLabelsUnique(r)
}

// insertExemplars inserts exemplars into the provided ts slice, in every-th position.
func insertExemplars(ts []prompb.TimeSeries, every int) []prompb.TimeSeries {
	if len(ts) == 0 {
		panic("ts must not be empty")
	}
	for i := 0; i < len(ts); i++ {
		if i%every == 0 {
			// Introduce exemplar by respecting every.
			exemplar_1 := prompb.Exemplar{
				Value:     float64(rand.Intn(100)),
				Timestamp: ts[i].Samples[0].Timestamp,
				Labels:    randomLabelsCount(0, 3), // Have labels in exemplars from 0 to 3.
			}
			exemplar_2 := prompb.Exemplar{
				Value:     float64(rand.Intn(100)),
				Timestamp: ts[i].Samples[1].Timestamp,
				Labels:    randomLabelsCount(0, 3), // Have labels in exemplars from 0 to 3.
			}
			ts[i].Exemplars = []prompb.Exemplar{exemplar_1, exemplar_2}
		}
	}
	return ts
}
