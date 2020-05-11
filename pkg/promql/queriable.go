package promql

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
)

func NewQueriable(reader pgmodel.Reader) *Queriable {
	return &Queriable{reader}
}

type Queriable struct {
	reader pgmodel.Reader
}

func (q Queriable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &querier{ctx, mint, maxt, q.reader}, nil
}

type querier struct {
	ctx        context.Context
	mint, maxt int64
	reader     pgmodel.Reader
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
	fmt.Println("closing querier")
	return nil
}

func (q querier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	query, err := prompb.ToQuery(q.mint, q.maxt, matchers, hints)
	if err != nil {
		return nil, nil, err
	}

	resp, err := q.reader.Read(&prompb.ReadRequest{
		Queries: []*prompb.Query{query},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pgModel.Reader: %v", err)
	}

	// don't hate on me
	//https://github.com/prometheus/prometheus/blob/master/storage/remote/client.go#L136
	result := resp.Results[0]
	return prompb.FromQueryResult(sortSeries, result), nil, nil
}
