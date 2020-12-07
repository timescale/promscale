package end_to_end_tests

import (
	"io/ioutil"
	"os"

	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	startTime int64 = 1577836800000
	endTime   int64 = 1577886800000

	samplesStartTime = 1597055698698
	samplesEndTime   = 1597059548699
)

var (
	wr prompb.WriteRequest
)

func generateSmallTimeseries() []prompb.TimeSeries {
	return []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "firstMetric"},
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
				{Name: pgmodel.MetricNameLabelName, Value: "secondMetric"},
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
				{Name: pgmodel.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_1"},
				{Name: "foo", Value: "bar"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "2"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_2"},
				{Name: "foo", Value: "bat"},
				{Name: "instance", Value: "3"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_3"},
				{Name: "instance", Value: "1"},
			},
		},
		{
			Labels: []prompb.Label{
				{Name: pgmodel.MetricNameLabelName, Value: "metric_3"},
				{Name: "instance", Value: "2"},
			},
		},
	}

	for i := range metrics {
		metrics[i].Samples = generateSamples(i + 1)
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
