package query

import (
	"context"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/promql"
)

func NewQueryable(q pgmodel.Querier) *Queryable {
	return &Queryable{q}
}

type Queryable struct {
	q pgmodel.Querier
}

func (q Queryable) Querier(ctx context.Context, mint, maxt int64) (promql.Querier, error) {
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
	lVals, err := q.pgQuerier.LabelValues(name)
	return lVals, nil, err
}

func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	lNames, err := q.pgQuerier.LabelNames()
	return lNames, nil, err
}

func (q querier) Close() error {
	return nil
}

func (q querier) Select(sortSeries bool, hints *storage.SelectHints, path []parser.Node, matchers ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	return q.pgQuerier.Select(q.mint, q.maxt, sortSeries, hints, path, matchers...)
}
