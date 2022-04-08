package adapters

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/util"
)

var samplesIngested = metrics.IngestorItems.With(map[string]string{"type": "metric", "kind": "sample", "subsystem": "rules"})

type ingestAdapter struct {
	ingestor *ingestor.DBIngestor
}

func NewIngestAdapter(ingestor *ingestor.DBIngestor) *ingestAdapter {
	return &ingestAdapter{ingestor}
}

type appenderAdapter struct {
	data     map[string][]model.Insertable
	ingestor *ingestor.DBIngestor
}

func (a ingestAdapter) Appender(_ context.Context) storage.Appender {
	return &appenderAdapter{
		data:     make(map[string][]model.Insertable),
		ingestor: a.ingestor,
	}
}

func (app *appenderAdapter) Append(_ storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	series, metricName, err := app.ingestor.SCache.GetSeriesFromProtos(util.LabelToPrompbLabels(l))
	if err != nil {
		return 0, fmt.Errorf("error creating series: %w", err)
	}

	samples := model.NewPromSamples(series, []prompb.Sample{{Timestamp: t, Value: v}})
	if _, found := app.data[metricName]; !found {
		app.data[metricName] = make([]model.Insertable, 0)
	}
	app.data[metricName] = append(app.data[metricName], samples)
	return 0, nil
}

func (app *appenderAdapter) AppendExemplar(_ storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	// We do not support appending exemplars in recording rules since this is not yet implemented upstream.
	// Once upstream implements this feature, we can modify this function.
	return 0, nil
}

func (app *appenderAdapter) Commit() error {
	numInsertablesIngested, err := app.ingestor.Dispatcher().InsertTs(context.Background(), model.Data{Rows: app.data, ReceivedTime: time.Now()})
	if err == nil {
		samplesIngested.Add(float64(numInsertablesIngested))
	}
	return errors.WithMessage(err, "rules: error ingesting data into db-ingestor")
}

func (app *appenderAdapter) Rollback() error {
	app.data = nil
	return nil
}
