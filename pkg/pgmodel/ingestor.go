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
	AddLabel(*prompb.Label)
	InsertLabels() ([]*prompb.Label, error)
	AddSeries(fingerprint uint64, series *model.LabelSet)
	InsertSeries() ([]SeriesID, []uint64, error)
	InsertData(rows map[string][][]interface{}) (uint64, error)
}

// Cache provides a caching mechanism for labels and series.
type Cache interface {
	GetLabel(*prompb.Label) (int32, error)
	SetLabel(*prompb.Label, int32) error
	GetSeries(fingerprint uint64) (SeriesID, error)
	SetSeries(fingerprint uint64, id SeriesID) error
}

type samplesInfo struct {
	seriesID    SeriesID
	fingerprint uint64
	samples     []prompb.Sample
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

func (i *DBIngestor) parseData(tts []*prompb.TimeSeries) (map[string][][]interface{}, error) {
	var err error
	dataSamples := make(map[string][]samplesInfo, 0)

	for _, t := range tts {
		if len(t.Samples) == 0 {
			continue
		}

		metricName := ""
		metric := make(model.LabelSet, len(t.Labels))
		newSeries := false
		for _, l := range t.Labels {
			_, err := i.cache.GetLabel(&l)
			if err != nil {
				if err != ErrEntryNotFound {
					return nil, err
				}
				i.db.AddLabel(&l)
				newSeries = true
			}
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)

			if l.Name == metricNameLabelName {
				metricName = l.Name
			}
		}
		fingerprint := uint64(metric.Fingerprint())
		var seriesID SeriesID
		if !newSeries {
			seriesID, err = i.cache.GetSeries(fingerprint)
			if err != nil {
				if err != ErrEntryNotFound {
					return nil, err
				}
				newSeries = true
			}
		}

		if newSeries {
			i.db.AddSeries(fingerprint, &metric)
		}

		if metricName == "" {
			return nil, errNoMetricName
		}

		if _, ok := dataSamples[metricName]; !ok {
			dataSamples[metricName] = make([]samplesInfo, 0)
		}

		dataSamples[metricName] = append(
			dataSamples[metricName],
			samplesInfo{
				seriesID,
				fingerprint,
				t.Samples,
			},
		)
	}

	if err = i.insertLabels(); err != nil {
		return nil, err
	}

	if err = i.insertSeries(); err != nil {
		return nil, err
	}

	dataRows := make(map[string][][]interface{}, 0)

	for metricName, infos := range dataSamples {
		dataRows[metricName] = make([][]interface{}, 0, len(infos))
		for _, info := range infos {
			if info.seriesID == 0 {
				// At this point, all series should be in cache.
				if info.seriesID, err = i.cache.GetSeries(info.fingerprint); err != nil {
					return nil, err
				}
			}
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

func (i *DBIngestor) insertLabels() error {
	ll, err := i.db.InsertLabels()
	if err != nil {
		return err
	}

	for _, label := range ll {
		if err := i.cache.SetLabel(label, 0); err != nil {
			return err
		}
	}

	return nil
}

func (i *DBIngestor) insertSeries() error {
	ids, fps, err := i.db.InsertSeries()

	if err != nil {
		return err
	}

	for idx, fp := range fps {
		err := i.cache.SetSeries(fp, ids[idx])
		if err != nil {
			return err
		}
	}

	return nil
}
