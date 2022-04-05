package adapters

import (
	"context"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/prompb"
)

type ingestAdapter struct {
	ingestor *ingestor.DBIngestor
}

func NewIngestAdapter(ingestor *ingestor.DBIngestor) storage.Appendable {
	return ingestAdapter{ingestor}
}

type appenderAdapter struct {
	data map[uint64]*prompb.TimeSeries
}

func (a ingestAdapter) Appender(_ context.Context) storage.Appender {
	return &appenderAdapter{
		data: make(map[uint64]*prompb.TimeSeries),
	}
}

func (app *appenderAdapter) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	hash := l.Hash()
	if _, found := app.data[hash]; !found {
		app.data[hash] = initTs(l)
	}
	app.data[hash].Samples = append(app.data[hash].Samples, prompb.Sample{Timestamp: t, Value: v})
	return 0, nil // return a 0 seriesRef since we do not do series level caching.
}

func (app *appenderAdapter) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	// todo
	return 0, nil
}

func initTs(l labels.Labels) *prompb.TimeSeries {
	return &prompb.TimeSeries{
		Labels:    labelToPrompbLabels(l),
		Samples:   make([]prompb.Sample, 0),
		Exemplars: make([]prompb.Exemplar, 0),
	}
}

func labelToPrompbLabels(l labels.Labels) []prompb.Label {
	if len(l) == 0 {
		return []prompb.Label{}
	}
	lbls := make([]prompb.Label, len(l))
	for i := range l {
		lbls[i].Name = l[i].Name
		lbls[i].Value = l[i].Value
	}
	return lbls
}

func (app *appenderAdapter) Commit() error {
	// todo
	return nil
}

func (app *appenderAdapter) Rollback() error {
	app.data = nil
	return nil
}
