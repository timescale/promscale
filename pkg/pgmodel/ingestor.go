package pgmodel

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	// Index of the series ID in the data rows.
	seriesIdx = 2
)

// Inserter is responsible for inserting label, series and data into the storage.
type Inserter interface {
	AddLabel(*prompb.Label)
	InsertLabels() ([]*prompb.Label, error)
	AddSeries(fingerprint uint64, series *model.LabelSet)
	InsertSeries() ([]uint64, []uint64, error)
	InsertData(rows [][]interface{}) (uint64, error)
}

// Cache provides a caching mechanism for labels and series.
type Cache interface {
	GetLabel(*prompb.Label) (int32, error)
	SetLabel(*prompb.Label, int32) error
	GetSeries(fingerprint uint64) (uint64, error)
	SetSeries(fingerprint uint64, id uint64) error
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	cache Cache
	db    Inserter
}

// Ingest transforms and ingests the timeseries data into Timescale database.
func (i *DBIngestor) Ingest(tts []*prompb.TimeSeries) (uint64, error) {
	var rowsIndex uint64
	var err error
	dataRows := make([][]interface{}, 0, len(tts))
	samplesMissingID := make(map[uint64]uint64, 0)

	for _, t := range tts {
		if len(t.Samples) == 0 {
			continue
		}

		metric := make(model.LabelSet, len(t.Labels))
		newSeries := false
		for _, l := range t.Labels {
			_, err := i.cache.GetLabel(&l)
			if err != nil {
				if err != ErrEntryNotFound {
					return 0, err
				}
				i.db.AddLabel(&l)
				newSeries = true
			}
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}
		fingerprint := uint64(metric.Fingerprint())
		var seriesID uint64
		if !newSeries {
			seriesID, err = i.cache.GetSeries(fingerprint)
			if err != nil {
				if err != ErrEntryNotFound {
					return 0, err
				}
				newSeries = true
			}
		}

		if newSeries {
			i.db.AddSeries(fingerprint, &metric)
		}

		for _, sample := range t.Samples {
			samplesMissingID[rowsIndex] = fingerprint
			dataRows = append(
				dataRows,
				[]interface{}{
					model.Time(sample.Timestamp).Time(),
					sample.Value,
					seriesID,
				},
			)
			rowsIndex++
		}
	}

	if err = i.insertLabels(); err != nil {
		return 0, err
	}

	err = i.insertSeries()
	if err != nil {
		return 0, err
	}

	// Update missing series IDs.
	var seriesID uint64
	for row, fp := range samplesMissingID {
		if seriesID, err = i.cache.GetSeries(fp); err != nil {
			return 0, err
		}
		dataRows[row][seriesIdx] = seriesID
	}

	return i.db.InsertData(dataRows)
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
