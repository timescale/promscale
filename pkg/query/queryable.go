// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
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

type samplesQuerier struct {
	ctx           context.Context
	mint, maxt    int64
	metricsReader pgQuerier.Querier
	labelsReader  lreader.LabelsReader
	seriesSets    []pgQuerier.SeriesSet
}

func (q queryable) ExemplarsQuerier(ctx context.Context) pgQuerier.ExemplarQuerier {
	return q.querier.ExemplarsQuerier(ctx)
}

func (q queryable) SamplesQuerier(ctx context.Context, mint, maxt int64) (promql.SamplesQuerier, error) {
	return q.newSamplesQuerier(ctx, mint, maxt), nil
}

func (q queryable) newSamplesQuerier(ctx context.Context, mint, maxt int64) *samplesQuerier {
	return &samplesQuerier{
		ctx: ctx, mint: mint, maxt: maxt,
		metricsReader: q.querier,
		labelsReader:  q.labelsReader,
	}
}

func (q samplesQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	lVals, err := q.labelsReader.LabelValues(name)
	return lVals, nil, err
}

func (q samplesQuerier) LabelNames(_ ...*labels.Matcher) ([]string, storage.Warnings, error) {
	// todo: implement labels matcher
	lNames, err := q.labelsReader.LabelNames()
	return lNames, nil, err
}

func (q *samplesQuerier) Close() {
	for _, ss := range q.seriesSets {
		ss.Close()
	}
}

func (q *samplesQuerier) Select(sortSeries bool, hints *storage.SelectHints, qh *pgQuerier.QueryHints, path []parser.Node, matchers ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	qry := q.metricsReader.SamplesQuerier()
	ss, n := qry.Select(q.mint, q.maxt, sortSeries, hints, qh, path, matchers...)
	q.seriesSets = append(q.seriesSets, ss)
	return ss, n
}
