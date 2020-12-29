// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type Parser interface {
	ParseData([]prompb.TimeSeries) (map[string][]model.Samples, int, error)
}

type dataParser struct {
	scache cache.SeriesCache
}

func DefaultParser(seriesCache cache.SeriesCache) Parser {
	return &dataParser{scache: seriesCache}
}

// Parse data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
func (d *dataParser) ParseData(tts []prompb.TimeSeries) (map[string][]model.Samples, int, error) {
	dataSamples := make(map[string][]model.Samples)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := d.scache.GetSeriesFromProtos(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, errors.ErrNoMetricName
		}
		sample := model.NewPromSample(seriesLabels, t.Samples)
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	return dataSamples, rows, nil
}
