package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/query"
)

func TestSeries(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})

	testCases := []struct {
		name        string
		querier     *mockQuerier
		matchers    []string
		start       string
		end         string
		expectCode  int
		expectError string
	}{
		{
			name:        "match[] is not sent",
			expectCode:  http.StatusBadRequest,
			matchers:    []string{},
			start:       "1",
			end:         "2",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Start is unparsable",
			expectCode:  http.StatusBadRequest,
			matchers:    []string{"m"},
			start:       "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "End is unparsable",
			expectCode:  http.StatusBadRequest,
			matchers:    []string{"m"},
			start:       "1.1",
			end:         "unparsable",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "End is before start",
			expectCode:  http.StatusBadRequest,
			matchers:    []string{"m"},
			start:       "1.1",
			end:         "1",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Matcher is unparsable",
			expectCode:  http.StatusBadRequest,
			matchers:    []string{"wrong_matcher{"},
			start:       "1",
			end:         "2",
			expectError: "bad_data",
			querier:     &mockQuerier{},
		}, {
			name:        "Select error",
			start:       "1",
			end:         "2",
			expectCode:  http.StatusUnprocessableEntity,
			expectError: "execution",
			matchers:    []string{"m"},
			querier:     &mockQuerier{selectErr: fmt.Errorf("some error")},
		}, {
			name:       "All good",
			start:      "1",
			end:        "2",
			expectCode: http.StatusOK,
			matchers:   []string{"m", `m{a="1"}`},
			querier:    &mockQuerier{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := series(query.NewQueryable(tc.querier))
			queryUrl := constructSeriesRequest(tc.start, tc.end, tc.matchers)
			w := doSeriesRequest(t, handler, queryUrl)

			if w.Code != tc.expectCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, tc.expectCode)
				return
			}
			if tc.expectError != "" {
				var er errResponse
				_ = json.NewDecoder(bytes.NewReader(w.Body.Bytes())).Decode(&er)
				if tc.expectError != er.ErrorType {
					t.Errorf("expected error of type %s, got %s", tc.expectError, er.ErrorType)
				}
			}
		})

	}

}

func constructSeriesRequest(start, end string, matchers []string) string {
	var matcherParams []string
	for _, m := range matchers {
		matcherParams = append(matcherParams, "match[]="+m)
	}
	match := strings.Join(matcherParams, "&")
	return fmt.Sprintf(
		"http://localhost:9090/series?start=%s&end=%s&%s",
		start, end, match,
	)
}

func doSeriesRequest(t *testing.T, queryHandler http.Handler, url string) *httptest.ResponseRecorder {
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded; param=value",
	)
	w := httptest.NewRecorder()
	queryHandler.ServeHTTP(w, req)
	return w
}
