package promql

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
)

func NewQueryable(q pgmodel.Querier) *Queryable {
	return &Queryable{q}
}

type Queryable struct {
	q pgmodel.Querier
}

func (q Queryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return newQuerier(ctx, q.q, mint, maxt)
}

type querier struct {
	ctx        context.Context
	mint, maxt int64
	pgQuerier  pgmodel.Querier
}

func newQuerier(ctx context.Context, q pgmodel.Querier, mint, maxt int64) (*querier, error) {
	return &querier{ctx, mint, maxt, q}, nil
}

func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	fmt.Println("querier label values: ", name)
	return nil, nil, nil
}

func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	fmt.Println("querier label names")
	return nil, nil, nil
}

func (q querier) Close() error {
	return nil
}

func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	return q.pgQuerier.Select(q.mint, q.maxt, sortSeries, hints, matchers...)
}
