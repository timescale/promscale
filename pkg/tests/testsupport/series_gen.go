// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package testsupport

import (
	"fmt"
	"math/rand"

	"github.com/timescale/promscale/pkg/prompb"
)

type seriesGen struct {
	ts []prompb.TimeSeries
}

const (
	labelKeyPrefix   = "test_k_"
	labelValuePrefix = "test_v_"
	metricNamePrefix = "test_metric_"
)

var samples = []prompb.Sample{{Timestamp: 1, Value: 1}}

// NewSeriesGenerator generates distinct timeseries equal to numMetrics * numSeriesPerMetric, such that
// each timeseries has `labels` count of labels. The label keys are common across the series, but their
// values are random, thereby creating a new series.
func NewSeriesGenerator(numMetrics, numSeriesPerMetric, labels int) (*seriesGen, error) {
	if labels < 2 {
		return nil, fmt.Errorf("minLabels cannot be less than 2")
	}
	totalSeries := numMetrics * numSeriesPerMetric
	ts := make([]prompb.TimeSeries, 0, totalSeries)

	labels -= 1 // Since 1 label will be occupied by __name__
	labelKeys := make([]string, labels)
	for i := 0; i < labels; i++ {
		labelKeys[i] = fmt.Sprintf("%s%d", labelKeyPrefix, i)
	}

	for i := 0; i < numMetrics; i++ {
		metricName := randomText(metricNamePrefix)
		metric := prompb.Label{Name: "__name__", Value: metricName}

		for j := 0; j < numSeriesPerMetric; j++ {
			serie := []prompb.Label{metric}

			for k := 0; k < labels; k++ {
				// The keys remain the same across the series, but the values change, everytime creating a new series.
				serie = append(serie, prompb.Label{Name: labelKeys[k], Value: randomText(labelValuePrefix)})
			}
			ts = append(ts, prompb.TimeSeries{
				Labels:  serie,
				Samples: samples,
			})
		}
	}
	return &seriesGen{ts}, nil
}

func (s *seriesGen) GetTimeseriesInBatch(batchSize int) [][]prompb.TimeSeries {
	ts := s.ts
	batches := make([][]prompb.TimeSeries, 0, (len(ts)+batchSize-1)/batchSize)

	for batchSize < len(ts) {
		ts, batches = ts[batchSize:], append(batches, ts[0:batchSize:batchSize])
	}
	batches = append(batches, ts)
	return batches
}

func randomText(prefix string) string {
	suffix := rand.Int() // #nosec
	return fmt.Sprintf("%s%d", prefix, suffix)
}
