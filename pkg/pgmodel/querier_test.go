// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package pgmodel

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/prompb"
)

type mockQuerier struct {
	tts               []*prompb.TimeSeries
	err               error
	healthCheckCalled bool
	labelNames        []string
	labelNamesErr     error
}

var _ Querier = (*mockQuerier)(nil)

func (q *mockQuerier) Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, path []parser.Node, ms ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	return nil, nil
}

func (q *mockQuerier) Query(*prompb.Query) ([]*prompb.TimeSeries, error) {
	return q.tts, q.err
}

func (q *mockQuerier) LabelNames() ([]string, error) {
	return q.labelNames, q.labelNamesErr
}

func (q *mockQuerier) LabelValues(string) ([]string, error) {
	return nil, nil
}

func (q *mockQuerier) HealthCheck() error {
	q.healthCheckCalled = true
	return nil
}

func (q *mockQuerier) NumCachedLabels() int {
	return 0
}

func (q *mockQuerier) LabelsCacheCapacity() int {
	return 0
}

func TestDBReaderRead(t *testing.T) {
	testCases := []struct {
		name string
		req  *prompb.ReadRequest
		tts  []*prompb.TimeSeries
		err  error
	}{
		{
			name: "No request",
		},
		{
			name: "No queries",
			req: &prompb.ReadRequest{
				Queries: []*prompb.Query{},
			},
		},
		{
			name: "Query error",
			req: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{StartTimestampMs: 1},
				},
			},
			err: fmt.Errorf("some error"),
		},
		{
			name: "Simple query, no results",
			req: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{StartTimestampMs: 1},
				},
			},
		},
		{
			name: "Simple query",
			req: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{StartTimestampMs: 1},
				},
			},
			tts: []*prompb.TimeSeries{
				{
					Samples: []prompb.Sample{
						{
							Timestamp: 1,
							Value:     2,
						},
					},
				},
			},
		},
		{
			name: "Multiple queries",
			req: &prompb.ReadRequest{
				Queries: []*prompb.Query{
					{StartTimestampMs: 1},
					{StartTimestampMs: 1},
					{StartTimestampMs: 1},
					{StartTimestampMs: 1},
				},
			},
			tts: []*prompb.TimeSeries{
				{
					Samples: []prompb.Sample{
						{
							Timestamp: 1,
							Value:     2,
						},
					},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mq := &mockQuerier{
				tts: c.tts,
				err: c.err,
			}

			r := DBReader{mq}

			res, err := r.Read(c.req)

			if err != nil {
				if c.err == nil || err != c.err {
					t.Errorf("unexpected error:\ngot\n%s\nwanted\n%s\n", err, c.err)
				}
				return
			}

			if c.req == nil {
				if res != nil {
					t.Errorf("unexpected result:\ngot\n%v\nwanted\n%v", res, nil)
				}
				return
			}

			expRes := &prompb.ReadResponse{
				Results: make([]*prompb.QueryResult, len(c.req.Queries)),
			}

			for i := range c.req.Queries {
				expRes.Results[i] = &prompb.QueryResult{
					Timeseries: c.tts,
				}
			}

			if !reflect.DeepEqual(res, expRes) {
				t.Errorf("unexpected result:\ngot\n%v\nwanted\n%v", res, expRes)
			}

		})
	}

}

func TestHealthCheck(t *testing.T) {
	mq := &mockQuerier{}

	r := DBReader{mq}

	err := r.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}

	if !mq.healthCheckCalled {
		t.Fatal("health check method not called when expected")
	}
}
