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

// NewIngestAdapter acts as an adapter to make Promscale's DBIngestor compatible with storage.Appendable
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
	series, metricName, err := app.ingestor.SeriesCache().GetSeriesFromProtos(util.LabelToPrompbLabels(l))
	if err != nil {
		return 0, fmt.Errorf("get series from protos: %w", err)
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
	// Note: InsertTs does 2 things:
	// 1. Ingest series
	// 2. Ingest samples
	//
	// An error might occur while ingesting samples, so Prometheus will call the app.Rollback(). Do note that we cannot
	// rollback the ingested series, rather only ingested samples since they were the last step that created the error.
	numInsertablesIngested, err := app.ingestor.Dispatcher().InsertTs(context.Background(), model.Data{Rows: app.data, ReceivedTime: time.Now()})
	if err == nil {
		samplesIngested.Add(float64(numInsertablesIngested))
	}
	return errors.WithMessage(err, "rules: error ingesting data into db-ingestor")
}

func (app *appenderAdapter) Rollback() error {
	app.data = map[string][]model.Insertable{}
	app.ingestor = nil
	return nil
}
