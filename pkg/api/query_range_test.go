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
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func TestRangedQuery(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})
	testCases := []struct {
		name        string
		timeout     string
		querier     *mockQuerier
		metric      string
		start       string
		end         string
		step        string
		expectCode  int
		expectError string
		canceled    bool
	}{
		{
			name:        "Start is unparsable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "End is unparsable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1.1",
			end:         "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "End is before start",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1.1",
			end:         "1",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Step is unparasable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1",
			end:         "2",
			step:        "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Step is non-positive",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1",
			end:         "2",
			step:        "0s",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Resolution is too high",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1",
			end:         "11002",
			step:        "1s",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Timeout is unparsable",
			expectCode:  http.StatusBadRequest,
			metric:      "m",
			start:       "1",
			end:         "2.2",
			timeout:     "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "No query given",
			expectCode:  http.StatusBadRequest,
			metric:      "",
			start:       "1",
			end:         "2",
			step:        "1s",
			timeout:     "1m",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Timeout query",
			start:       "1",
			end:         "2",
			step:        "1s",
			expectCode:  http.StatusServiceUnavailable,
			expectError: "timeout",
			timeout:     "1s",
			metric:      "m",
			querier: &mockQuerier{
				timeToSleepOnSelect: 2 * time.Second,
			},
		}, {
			name:        "Cancel query",
			start:       "1",
			end:         "2",
			step:        "1s",
			expectCode:  http.StatusServiceUnavailable,
			expectError: "canceled",
			metric:      "m",
			querier:     &mockQuerier{},
			canceled:    true,
		}, {
			name:        "Select error",
			start:       "1",
			end:         "2",
			step:        "1s",
			expectCode:  http.StatusUnprocessableEntity,
			expectError: "execution",
			metric:      "m",
			querier:     &mockQuerier{selectErr: fmt.Errorf("some error")},
			timeout:     "30s",
		}, {
			name:       "All good",
			start:      "1",
			end:        "2",
			step:       "1s",
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
			handler := queryRange(engine, query.NewQueryable(tc.querier))
			queryUrl := constructRangedQuery(tc.metric, tc.start, tc.end, tc.step, tc.timeout)
			w := doRangedQuery(t, handler, queryUrl, tc.canceled)

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

func constructRangedQuery(metric, start, end, step, timeout string) string {
	return fmt.Sprintf(
		"http://localhost:9090/query_range?query=%s&start=%s&end=%s&step=%s&timeout=%s",
		metric, start, end, step, timeout,
	)
}

func doRangedQuery(t *testing.T, queryHandler http.Handler, url string, canceled bool) *httptest.ResponseRecorder {
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
