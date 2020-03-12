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
	InsertData(rows map[string][][]interface{}) (uint64, error)
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

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	cache Cache
	db    Inserter
}

// Ingest transforms and ingests the timeseries data into Timescale database.
func (i *DBIngestor) Ingest(tts []*prompb.TimeSeries) (uint64, error) {
	data, err := i.parseData(tts)

	if err != nil {
		return 0, err
	}

	return i.db.InsertData(data)
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

func (i *DBIngestor) parseData(tts []*prompb.TimeSeries) (map[string][][]interface{}, error) {
	var err error
	dataSamples := make(map[string][]*samplesInfo, 0)

	for _, t := range tts {
		if len(t.Samples) == 0 {
			continue
		}

		labels, metricName := labelProtosToLabels(t.Labels)
		if metricName == "" {
			return nil, errNoMetricName
		}

		var seriesID SeriesID
		newSeries := false

		seriesID, err = i.cache.GetSeries(labels)
		if err != nil {
			if err != ErrEntryNotFound {
				return nil, err
			}
			newSeries = true
		}

		sample := samplesInfo{
			seriesID,
			t.Samples,
		}

		if newSeries {
			i.db.AddSeries(labels, func(id SeriesID) error {
				sample.seriesID = id
				return i.cache.SetSeries(labels, id)
			})
		}

		if _, ok := dataSamples[metricName]; !ok {
			dataSamples[metricName] = make([]*samplesInfo, 0)
		}

		dataSamples[metricName] = append(dataSamples[metricName], &sample)
	}

	if err = i.db.InsertSeries(); err != nil {
		return nil, err
	}

	dataRows := make(map[string][][]interface{}, 0)

	for metricName, infos := range dataSamples {
		dataRows[metricName] = make([][]interface{}, 0, len(infos))
		for _, info := range infos {
			for _, sample := range info.samples {
				dataRows[metricName] = append(
					dataRows[metricName],
					[]interface{}{
						model.Time(sample.Timestamp).Time(),
						sample.Value,
						info.seriesID,
					},
				)
			}
		}
	}

	return dataRows, nil
}
