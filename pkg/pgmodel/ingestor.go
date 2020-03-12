package pgmodel

import (
	"fmt"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
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
	AddSeries(lset labels.Labels, callback func(id SeriesID) error)
	InsertSeries() error
	InsertData(rows map[string]*SampleInfoIterator) (uint64, error)
}

// Cache provides a caching mechanism for labels and series.
type Cache interface {
	GetSeries(lset labels.Labels) (SeriesID, error)
	SetSeries(lset labels.Labels, id SeriesID) error
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
	data, totalRows, err := i.parseData(tts)

	if err != nil {
		return 0, err
	}

	rowsInserted, err := i.db.InsertData(data)
	if err == nil && int(rowsInserted) != totalRows {
		return rowsInserted, fmt.Errorf("Failed to insert all the data! Expected: %d, Got: %d", totalRows, rowsInserted)
	}
	return rowsInserted, err
}

//mostly copied from prometheus codec.go, except for the name return
func labelProtosToLabels(labelPairs []prompb.Label) (labels.Labels, string) {
	result := make(labels.Labels, 0, len(labelPairs))
	metricName := ""
	for _, l := range labelPairs {
		result = append(result, labels.Label{
			Name:  l.Name,
			Value: l.Value,
		})
		if l.Name == metricNameLabelName {
			metricName = l.Value
		}
	}
	sort.Sort(result)
	return result, metricName
}

func (i *DBIngestor) parseData(tts []prompb.TimeSeries) (map[string]*SampleInfoIterator, int, error) {
	var err error
	dataSamples := make(map[string]*SampleInfoIterator, 0)
	rows := 0

	for _, t := range tts {
		if len(t.Samples) == 0 {
			continue
		}

		labels, metricName := labelProtosToLabels(t.Labels)
		if metricName == "" {
			return nil, rows, errNoMetricName
		}

		var seriesID SeriesID
		newSeries := false

		seriesID, err = i.cache.GetSeries(labels)
		if err != nil {
			if err != ErrEntryNotFound {
				return nil, rows, err
			}
			newSeries = true
		}

		sample := samplesInfo{
			seriesID,
			t.Samples,
		}
		rows += len(t.Samples)

		if newSeries {
			i.db.AddSeries(labels, func(id SeriesID) error {
				sample.seriesID = id
				return i.cache.SetSeries(labels, id)
			})
		}

		if _, ok := dataSamples[metricName]; !ok {
			dataSamples[metricName] = NewSampleInfoIterator()
		}

		dataSamples[metricName].Append(&sample)
	}

	if err = i.db.InsertSeries(); err != nil {
		return nil, rows, err
	}

	return dataSamples, rows, nil
}
