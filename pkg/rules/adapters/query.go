package adapters

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/promql"
)

type queryAdapter struct {
	queryable promql.Queryable
}

func NewQueryAdapter(q promql.Queryable) storage.Queryable {
	return queryAdapter{q}
}

func (q queryAdapter) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	qr, err := q.queryable.SamplesQuerier(ctx, mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("samples-querier: %w", err)
	}
	return querierAdapter{qr}, nil
}

type querierAdapter struct {
	qr promql.SamplesQuerier
}

func (q querierAdapter) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	// Pushdowns are not supported here. This is fine as Prometheus rule-manager only uses queryable to know
	// the previous state of the alert. This function is not used in recording/alerting rules evaluation.
	seriesSet, _ := q.qr.Select(sortSeries, hints, nil, nil, matchers...)
	return seriesSet
}

func (q querierAdapter) LabelValues(name string, _ ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// Weak TODO: We need to implement the matchers.
	return q.qr.LabelValues(name)
}

func (q querierAdapter) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return q.qr.LabelNames(matchers...)
}

func (q querierAdapter) Close() error {
	q.qr.Close()
	return nil
}
