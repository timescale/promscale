// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/prompb"
)

type mockQuerier struct {
	tts           []*prompb.TimeSeries
	err           error
	labelNames    []string
	labelNamesErr error
}

var _ querier.Querier = (*mockQuerier)(nil)

func (q *mockQuerier) SamplesQuerier() querier.SamplesQuerier {
	return mockSamplesQuerier{}
}

type mockSamplesQuerier struct{}

func (q mockSamplesQuerier) Select(mint int64, maxt int64, sortSeries bool, hints *storage.SelectHints, qh *querier.QueryHints, path []parser.Node, ms ...*labels.Matcher) (querier.SeriesSet, parser.Node) {
	return nil, nil
}

func (q *mockQuerier) RemoteReadQuerier() querier.RemoteReadQuerier {
	return mockRemoteReadQuerier{
		tts: q.tts,
		err: q.err,
	}
}

type mockRemoteReadQuerier struct {
	tts []*prompb.TimeSeries
	err error
}

func (q mockRemoteReadQuerier) Query(_ *prompb.Query) ([]*prompb.TimeSeries, error) {
	return q.tts, q.err
}

func (q *mockQuerier) ExemplarsQuerier(_ context.Context) querier.ExemplarQuerier {
	return nil
}

func (q *mockQuerier) LabelNames() ([]string, error) {
	return q.labelNames, q.labelNamesErr
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

			r := Client{querier: mq}

			res, err := r.Read(c.req)

			if err != nil {
				if c.err == nil || !errors.Is(err, c.err) {
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
	healthCheckCalled := false

	r := Client{healthCheck: func() error {
		healthCheckCalled = true
		return nil
	}}

	err := r.HealthCheck()
	if err != nil {
		t.Fatal(err)
	}

	if !healthCheckCalled {
		t.Fatal("health check method not called when expected")
	}
}
