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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/prompb"
	"github.com/timescale/timescale-prometheus/pkg/promql"
	"github.com/timescale/timescale-prometheus/pkg/query"
)

type mockSeriesSet struct{}

func (m mockSeriesSet) Next() bool {
	return false
}

func (m mockSeriesSet) At() storage.Series {
	return nil
}

func (m mockSeriesSet) Err() error {
	return nil
}

type mockQuerier struct {
	timeToSleep time.Duration
	err         error
}

func (m mockQuerier) Query(*prompb.Query) ([]*prompb.TimeSeries, error) {
	panic("implement me")
}

func (m mockQuerier) Select(int64, int64, bool, *storage.SelectHints, []parser.Node, ...*labels.Matcher) (storage.SeriesSet, parser.Node, storage.Warnings, error) {
	time.Sleep(m.timeToSleep)

	return &mockSeriesSet{}, nil, nil, m.err
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
			t.Errorf("unexected lack of error for input: %s", tc.in)
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
	_ = log.Init("debug")
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
				timeToSleep: 2 * time.Second,
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
			querier:     &mockQuerier{err: fmt.Errorf("some error")},
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
			handler := Query(engine, query.NewQueryable(tc.querier))
			queryUrl := constructQuery(tc.metric, tc.time, tc.timeout)
			w := doQuery(t, handler, queryUrl, tc.canceled)

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
