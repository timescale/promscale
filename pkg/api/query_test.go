package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

type mockSeriesSet struct {
	err error
}

func (m mockSeriesSet) Next() bool {
	return false
}

func (m mockSeriesSet) At() storage.Series {
	return nil
}

func (m mockSeriesSet) Err() error {
	return m.err
}

func (m mockSeriesSet) Warnings() storage.Warnings {
	return nil
}

type mockQuerier struct {
	timeToSleepOnSelect time.Duration
	selectErr           error
	labelNames          []string
	labelNamesErr       error
}

var _ pgmodel.Querier = (*mockQuerier)(nil)

func (m mockQuerier) LabelNames() ([]string, error) {
	return m.labelNames, m.labelNamesErr
}

func (m mockQuerier) LabelValues(string) ([]string, error) {
	return nil, nil
}

func (m mockQuerier) Query(*prompb.Query) ([]*prompb.TimeSeries, error) {
	panic("implement me")
}

func (m mockQuerier) Select(int64, int64, bool, *storage.SelectHints, []parser.Node, ...*labels.Matcher) (storage.SeriesSet, parser.Node) {
	time.Sleep(m.timeToSleepOnSelect)
	return &mockSeriesSet{err: m.selectErr}, nil
}

func (m mockQuerier) NumCachedLabels() int {
	return 0
}

func (m mockQuerier) LabelsCacheCapacity() int {
	return 0
}

func TestParseDuration(t *testing.T) {
	testCase := []struct {
		in          string
		out         time.Duration
		expectError bool
	}{
		{
			in:          fmt.Sprintf("%.6f", float64(math.MaxInt64)+1.0),
			expectError: true,
		}, {
			in:          fmt.Sprintf("%.6f", float64(math.MinInt64)-1.0),
			expectError: true,
		}, {
			in:  "1s",
			out: time.Second,
		}, {
			in:  "3h",
			out: 3 * time.Hour,
		}, {
			in:  "90m",
			out: 90 * time.Minute,
		},
	}

	for _, tc := range testCase {
		got, err := parseDuration(tc.in)
		if err == nil && tc.expectError {
			t.Errorf("unexpected lack of error for input: %s", tc.in)
			continue
		}
		if err != nil && !tc.expectError {
			t.Errorf("unexpected error '%v' for input %s", err, tc.in)
			continue
		}
		if tc.expectError {
			continue
		}
		if got != tc.out {
			t.Errorf("expected: %v, got %v, for input %s", tc.out, got, tc.in)
		}
	}
}

func TestQuery(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})
	testCases := []struct {
		name        string
		timeout     string
		querier     *mockQuerier
		metric      string
		time        string
		expectCode  int
		expectError string
		canceled    bool
	}{
		{
			name:        "Time is unparsable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			time:        "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Timeout is unparsable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			time:        "1s",
			timeout:     "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "No query given",
			expectCode:  http.StatusBadRequest,
			metric:      "",
			time:        "1s",
			timeout:     "1m",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Timeout query",
			expectCode:  http.StatusServiceUnavailable,
			expectError: "timeout",
			timeout:     "1s",
			metric:      "m",
			querier: &mockQuerier{
				timeToSleepOnSelect: 2 * time.Second,
			},
		}, {
			name:        "Cancel query",
			expectCode:  http.StatusServiceUnavailable,
			expectError: "canceled",
			metric:      "m",
			querier:     &mockQuerier{},
			canceled:    true,
		}, {
			name:        "Select error",
			expectCode:  http.StatusUnprocessableEntity,
			expectError: "execution",
			metric:      "m",
			querier:     &mockQuerier{selectErr: fmt.Errorf("some error")},
			timeout:     "30s",
		}, {
			name:       "All good",
			expectCode: http.StatusOK,
			metric:     "m",
			querier:    &mockQuerier{},
			timeout:    "30s",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timeout, _ := parseDuration(tc.timeout)
			engine := promql.NewEngine(
				promql.EngineOpts{
					Logger:     log.GetLogger(),
					Reg:        prometheus.NewRegistry(),
					MaxSamples: math.MaxInt32,
					Timeout:    timeout,
				},
			)
			handler := queryHandler(engine, query.NewQueryable(tc.querier))
			queryURL := constructQuery(tc.metric, tc.time, tc.timeout)
			w := doQuery(t, handler, queryURL, tc.canceled)

			if w.Code != tc.expectCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, tc.expectCode)
				return
			}
			if tc.expectError != "" {
				var er errResponse
				_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&er)
				if tc.expectError != er.ErrorType {
					t.Errorf("expected error of type %s, got %s", tc.expectError, er.ErrorType)
					return
				}
			}
		})

	}

}

func constructQuery(metric, time string, timeout string) string {
	return fmt.Sprintf("http://localhost:9090/query?query=%s&time=%s&timeout=%s", metric, time, timeout)
}

func doQuery(t *testing.T, queryHandler http.Handler, url string, canceled bool) *httptest.ResponseRecorder {
	ctx, cancelFunc := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded; param=value",
	)
	w := httptest.NewRecorder()
	if canceled {
		cancelFunc()
	} else {
		defer cancelFunc()
	}
	queryHandler.ServeHTTP(w, req)
	return w
}
