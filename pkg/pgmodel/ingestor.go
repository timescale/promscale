// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"fmt"

	"github.com/prometheus/common/model"
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
	InsertNewData(newSeries []SeriesWithCallback, rows map[string]*SampleInfoIterator) (uint64, error)
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

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	sampleInfos     []*samplesInfo
	sampleInfoIndex int
	sampleIndex     int
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() *SampleInfoIterator {
	return &SampleInfoIterator{sampleInfos: make([]*samplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0}
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s *samplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.sampleInfos) && t.sampleIndex >= len(t.sampleInfos[t.sampleInfoIndex].samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.sampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() ([]interface{}, error) {
	info := t.sampleInfos[t.sampleInfoIndex]
	sample := info.samples[t.sampleIndex]
	row := []interface{}{
		model.Time(sample.Timestamp).Time(),
		sample.Value,
		info.seriesID,
	}
	return row, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
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

func (i *DBIngestor) parseData(tts []prompb.TimeSeries) ([]SeriesWithCallback, map[string]*SampleInfoIterator, int, error) {
	var seriesToInsert []SeriesWithCallback
	dataSamples := make(map[string]*SampleInfoIterator)
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

		if _, ok := dataSamples[metricName]; !ok {
			dataSamples[metricName] = NewSampleInfoIterator()
		}

		dataSamples[metricName].Append(&sample)
	}

	return seriesToInsert, dataSamples, rows, nil
}
