// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"io/ioutil"
	"math/rand"
	"os"
	"time"

	"github.com/golang/snappy"
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
	wr              prompb.WriteRequest
	startTimeRecent = timestamp.FromTime(time.Now())
	endTimeRecent   = startTimeRecent + time.Minute.Milliseconds()*2
)

func generateSmallTimeseries() []prompb.TimeSeries {
	return []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "firstMetric"},
				{Name: "foo", Value: "bar"},
				{Name: "common", Value: "tag"},
				{Name: "empty", Value: ""},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.2},
				{Timestamp: 3, Value: 0.3},
				{Timestamp: 4, Value: 0.4},
				{Timestamp: 5, Value: 0.5},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "secondMetric"},
				{Name: "foo", Value: "baz"},
				{Name: "common", Value: "tag"},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 1.1},
				{Timestamp: 2, Value: 1.2},
				{Timestamp: 3, Value: 1.3},
				{Timestamp: 4, Value: 1.4},
				{Timestamp: 5, Value: 1.5},
			},
		},
	}
}

// This function is used to generate timeseries used for ingesting into
// Prometheus and the connector to verify same results are being returned.
func generateLargeTimeseries() []prompb.TimeSeries {
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
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_counter"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
			Samples: generateResettingSamples(1, 10),
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_counter"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
			Samples: generateResettingSamples(2, 3),
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "metric_counter"},
				{Name: "foo", Value: "baz"},
				{Name: "instance", Value: "1"},
			},
			Samples: generateResettingSamples(2, 113),
		},
	}

	for i := range metrics {
		if len(metrics[i].Samples) == 0 {
			metrics[i].Samples = generateSamples(i + 1)
		}
	}

	return metrics
}

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

func generateResettingSamples(index int, reset_every int) []prompb.Sample {
	var (
		delta           = float64(index * 2)
		timeDelta int64 = 30000
	)
	samples := make([]prompb.Sample, 0, 3)
	i := 0
	time := startTime + (timeDelta * int64(i))

	value := float64(0)

	for time < endTime {
		samples = append(samples, prompb.Sample{
			Timestamp: time,
			Value:     value,
		})
		i++
		value += delta
		if (i % reset_every) == 0 {
			value = 0
		}
		time = startTime + (timeDelta * int64(i))
	}

	return samples
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

// generateRealTimeseries is used to read the real-world dataset from an
// external file. The dataset was generated using endpoints that are provided
// by the PromLabs PromQL Compliance Tester:
// https://github.com/promlabs/promql-compliance-tester/
// http://demo.promlabs.com:10000
// http://demo.promlabs.com:10001
// http://demo.promlabs.com:10002
func generateRealTimeseries() []prompb.TimeSeries {
	if len(wr.Timeseries) == 0 {
		f, err := os.Open("../testdata/real-dataset.sz")
		if err != nil {
			panic(err)
		}

		compressed, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}

		data, err := snappy.Decode(nil, compressed)
		if err != nil {
			panic(err)
		}

		err = wr.Unmarshal(data)

		if err != nil {
			panic(err)
		}
	}

	return wr.Timeseries
}

func generateSmallMultiTenantTimeseries() ([]prompb.TimeSeries, []string) {
	return []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "firstMetric"},
				{Name: "foo", Value: "bar"},
				{Name: "common", Value: "tag"},
				{Name: "empty", Value: ""},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 0.1},
				{Timestamp: 2, Value: 0.2},
				{Timestamp: 3, Value: 0.3},
				{Timestamp: 4, Value: 0.4},
				{Timestamp: 5, Value: 0.5},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "secondMetric"},
				{Name: "job", Value: "baz"},
				{Name: "ins", Value: "tag"},
			},
			Samples: []prompb.Sample{
				{Timestamp: 1, Value: 2.1},
				{Timestamp: 2, Value: 2.2},
				{Timestamp: 3, Value: 2.3},
				{Timestamp: 4, Value: 2.4},
				{Timestamp: 5, Value: 2.5},
			},
		},
	}, []string{"tenant-a", "tenant-b", "tenant-c"}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randomStr(numChars int) string {
	b := make([]byte, numChars)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func generateRandomMetricMetadata(num int) []prompb.MetricMetadata {
	randomMetricType := func() prompb.MetricMetadata_MetricType {
		// Generate any metric type from COUNTER to STATESET.
		return prompb.MetricMetadata_MetricType(rand.Intn(int(prompb.MetricMetadata_STATESET)-int(prompb.MetricMetadata_COUNTER)) + 1)
	}

	data := make([]prompb.MetricMetadata, num)
	prefixMetric := "metric_name_"
	prefixHelp := "help_"
	prefixUnit := "unit_"

	for i := 0; i < num; i++ {
		metadata := prompb.MetricMetadata{
			MetricFamilyName: prefixMetric + randomStr(10),
			Type:             randomMetricType(),
			Unit:             prefixUnit + randomStr(5),
			Help:             prefixHelp + randomStr(50),
		}
		data[i] = metadata
	}
	return data
}
