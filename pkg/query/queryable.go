// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	mq "github.com/timescale/promscale/pkg/pgmodel/querier"
	pgQuerier "github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/promql"
)

func NewQueryable(q pgQuerier.Querier, labelsReader lreader.LabelsReader) promql.Queryable {
	return &queryable{querier: q, labelsReader: labelsReader}
}

type queryable struct {
	querier      pgQuerier.Querier
	labelsReader lreader.LabelsReader
}

func (q queryable) newQuerier(ctx context.Context, mint, maxt int64) *querier {
	return &querier{
		ctx: ctx, mint: mint, maxt: maxt,
		metricsReader: q.querier,
		labelsReader:  q.labelsReader,
	}
}

func (q queryable) Samples(ctx context.Context, mint, maxt int64) (promql.SamplesQuerier, error) {
	return q.newQuerier(ctx, mint, maxt), nil
}

// todo: optimize this (remove need for querier)
func (q queryable) Exemplar(ctx context.Context) promql.ExemplarQuerier {
	return q.newQuerier(nil, 0, 0).metricsReader.Exemplar(ctx)
}

type querier struct {
	ctx           context.Context
	mint, maxt    int64
	metricsReader pgQuerier.Querier
	labelsReader  lreader.LabelsReader
	seriesSets    []pgQuerier.SeriesSet
}

func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	lVals, err := q.labelsReader.LabelValues(name)
	return lVals, nil, err
}

func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	lNames, err := q.labelsReader.LabelNames()
	return lNames, nil, err
}

func (q *querier) Close() error {
	for _, ss := range q.seriesSets {
		ss.Close()
	}
	return nil
}

func (q *querier) Select(sortSeries bool, hints *storage.SelectHints, qh *mq.QueryHints, path []parser.Node, matchers ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	ss, n := q.metricsReader.Select(q.mint, q.maxt, sortSeries, hints, qh, path, matchers...)
	q.seriesSets = append(q.seriesSets, ss)
	return ss, n
}
