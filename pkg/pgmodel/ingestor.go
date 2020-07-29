// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/timescale/timescale-prometheus/pkg/prompb"
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

type seriesWithCallback struct {
	Series   Labels
	Callback func(l Labels, id SeriesID) error
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

	seen := make(map[uintptr]struct{})
	for _, t := range tts {
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
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&t.Samples))
		if _, found := seen[hdr.Data]; found {
			fmt.Printf("duplicate in parseData %v\n", hdr.Data)
			panic("invalid")
		}
		seen[hdr.Data] = struct{}{}
		sample := samplesInfo{
			seriesLabels,
			-1, //sentinel marking the seriesId as unset
			t.Samples,
		}
		rows += len(t.Samples)
		checkForDuplicates("parseData", sample)
		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	for _, s := range dataSamples {
		checkForDuplicatesStr("parseData 2", s)
	}

	// FinishWriteRequest(req)

	return dataSamples, rows, nil
}

// Close closes the ingestor
func (i *DBIngestor) Close() {
	i.db.Close()
}

func checkForDuplicates(loc string, samples samplesInfo) {
	set := make(map[int64]struct{}, len(samples.samples))
	for _, sample := range samples.samples {
		_, found := set[sample.Timestamp]
		if found {
			fmt.Printf("%s: duplicate found %v %v \n", loc, sample.Timestamp, samples.labels)
		}
		set[sample.Timestamp] = struct{}{}
	}
}

func checkForDuplicatesStr(loc string, data []samplesInfo) {
	set := make(map[string]map[int64]float64, len(data))
	for i, samples := range data {
		if _, found := set[samples.labels.String()]; !found {
			set[samples.labels.String()] = make(map[int64]float64, len(samples.samples))
		}
		for j, sample := range samples.samples {
			v, found := set[samples.labels.String()][sample.Timestamp]
			if found {
				fmt.Printf("%s: duplicate found (%d, %d) [%d] (%v: %v, %v) %v\n", loc, i, j, len(samples.samples), sample.Timestamp, v, sample.Value, samples.labels.values)
			}
			set[samples.labels.String()][sample.Timestamp] = sample.Value
		}
	}
}
