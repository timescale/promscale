// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/utils"
	"github.com/timescale/promscale/pkg/prompb"
)

var (
	ErrNoMetricName = fmt.Errorf("metric name missing")
)

// inserter is responsible for inserting label, series and data into the storage.
type inserter interface {
	InsertNewData(rows map[string][]utils.SamplesInfo) (uint64, error)
	CompleteMetricCreation() error
	Close()
}

// SeriesCache provides a caching mechanism for labels and series.
type SeriesCache interface {
	GetSeries(lset utils.Labels) (utils.SeriesID, error)
	SetSeries(lset utils.Labels, id utils.SeriesID) error
	NumElements() int
	Capacity() int
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	db inserter
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (i *DBIngestor) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	data, totalRows, err := i.parseData(tts, req)

	if err != nil {
		return 0, err
	}

	rowsInserted, err := i.db.InsertNewData(data)
	if err == nil && int(rowsInserted) != totalRows {
		return rowsInserted, fmt.Errorf("Failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return rowsInserted, err
}

// Parts of metric creation not needed to insert data
func (i *DBIngestor) CompleteMetricCreation() error {
	return i.db.CompleteMetricCreation()
}

// Parse data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// NOTE: req will be added to our WriteRequest pool in this function, it must
//       not be used afterwards.
func (i *DBIngestor) parseData(tts []prompb.TimeSeries, req *prompb.WriteRequest) (map[string][]utils.SamplesInfo, int, error) {
	dataSamples := make(map[string][]utils.SamplesInfo)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := utils.LabelProtosToLabels(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, ErrNoMetricName
		}
		sample := utils.SamplesInfo{
			seriesLabels,
			-1, // sentinel marking the seriesId as unset
			t.Samples,
		}
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(req)

	return dataSamples, rows, nil
}

// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}
