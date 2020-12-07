package query

import (
	"context"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/promql"
)

func NewQueryable(q pgmodel.Querier, labelsReader pgmodel.LabelsReader) promql.Queryable {
	return &queryable{querier: q, labelsReader: labelsReader}
}

type queryable struct {
	querier      pgmodel.Querier
	labelsReader pgmodel.LabelsReader
}

func (q queryable) Querier(ctx context.Context, mint, maxt int64) (promql.Querier, error) {
	return &querier{
		ctx: ctx, mint: mint, maxt: maxt,
		metricsReader: q.querier,
		labelsReader:  q.labelsReader,
	}, nil
}

type querier struct {
	ctx           context.Context
	mint, maxt    int64
	metricsReader pgmodel.Querier
	labelsReader  pgmodel.LabelsReader
}

func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	lVals, err := q.labelsReader.LabelValues(name)
	return lVals, nil, err
}

func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	lNames, err := q.labelsReader.LabelNames()
	return lNames, nil, err
}

func (q querier) Close() error {
	return nil
}

func (q querier) Select(sortSeries bool, hints *storage.SelectHints, path []parser.Node, matchers ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	return q.metricsReader.Select(q.mint, q.maxt, sortSeries, hints, path, matchers...)
}
