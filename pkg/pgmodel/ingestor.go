// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"fmt"

	"github.com/timescale/promscale/pkg/prompb"
)

const (
	MetricNameLabelName = "__name__"
)

var (
	ErrNoMetricName = fmt.Errorf("metric name missing")
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

// inserter is responsible for inserting label, series and data into the storage.
type inserter interface {
	InsertNewData(rows map[string][]samplesInfo) (uint64, error)
	CompleteMetricCreation() error
	Close()
}

// SeriesCache provides a caching mechanism for labels and series.
type SeriesCache interface {
	GetSeries(lset Labels) (SeriesID, error)
	SetSeries(lset Labels, id SeriesID) error
	NumElements() int
	Capacity() int
}

type samplesInfo struct {
	labels   *Labels
	seriesID SeriesID
	samples  []prompb.Sample
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	db inserter
}

// Ingest transforms and ingests the timeseries data into Timescale database.
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

func (i *DBIngestor) CompleteMetricCreation() error {
	return i.db.CompleteMetricCreation()
}

func (i *DBIngestor) parseData(tts []prompb.TimeSeries, req *prompb.WriteRequest) (map[string][]samplesInfo, int, error) {
	dataSamples := make(map[string][]samplesInfo)
	rows := 0

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		seriesLabels, metricName, err := labelProtosToLabels(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, ErrNoMetricName
		}
		sample := samplesInfo{
			seriesLabels,
			-1, //sentinel marking the seriesId as unset
			t.Samples,
		}
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	FinishWriteRequest(req)

	return dataSamples, rows, nil
}

// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}
