// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"fmt"

	"github.com/prometheus/prometheus/prompb"
)

const (
	metricNameLabelName = "__name__"
)

var (
	errNoMetricName = fmt.Errorf("metric name missing")
)

// SeriesID represents a globally unique id for the series. This should be equivalent
// to the PostgreSQL type in the series table (currently BIGINT).
type SeriesID int64

// Inserter is responsible for inserting label, series and data into the storage.
type Inserter interface {
	InsertNewData(newSeries []SeriesWithCallback, rows map[string][]*samplesInfo) (uint64, error)
	Close()
}

type SeriesWithCallback struct {
	Series   Labels
	Callback func(l Labels, id SeriesID) error
}

// Cache provides a caching mechanism for labels and series.
type Cache interface {
	GetSeries(lset Labels) (SeriesID, error)
	SetSeries(lset Labels, id SeriesID) error
}

type samplesInfo struct {
	seriesID SeriesID
	samples  []prompb.Sample
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	cache Cache
	db    Inserter
}

// Ingest transforms and ingests the timeseries data into Timescale database.
func (i *DBIngestor) Ingest(tts []prompb.TimeSeries) (uint64, error) {
	newSeries, data, totalRows, err := i.parseData(tts)

	if err != nil {
		return 0, err
	}

	rowsInserted, err := i.db.InsertNewData(newSeries, data)
	if err == nil && int(rowsInserted) != totalRows {
		return rowsInserted, fmt.Errorf("Failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return rowsInserted, err
}

func (i *DBIngestor) parseData(tts []prompb.TimeSeries) ([]SeriesWithCallback, map[string][]*samplesInfo, int, error) {
	var seriesToInsert []SeriesWithCallback
	dataSamples := make(map[string][]*samplesInfo, 0)
	rows := 0

	for _, t := range tts {
		if len(t.Samples) == 0 {
			continue
		}

		seriesLabels, metricName, err := labelProtosToLabels(t.Labels)
		if err != nil {
			return nil, nil, rows, err
		}
		if metricName == "" {
			return nil, nil, rows, errNoMetricName
		}

		var seriesID SeriesID
		newSeries := false

		seriesID, err = i.cache.GetSeries(seriesLabels)
		if err != nil {
			if err != ErrEntryNotFound {
				return nil, nil, rows, err
			}
			newSeries = true
		}

		sample := samplesInfo{
			seriesID,
			t.Samples,
		}
		rows += len(t.Samples)

		if newSeries {
			seriesToInsert = append(seriesToInsert, SeriesWithCallback{
				Series: seriesLabels,
				Callback: func(l Labels, id SeriesID) error {
					sample.seriesID = id
					return i.cache.SetSeries(l, id)
				},
			})
		}

		dataSamples[metricName] = append(dataSamples[metricName], &sample)
	}

	return seriesToInsert, dataSamples, rows, nil
}

func (i *DBIngestor) Close() {
	i.db.Close()
}
