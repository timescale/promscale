package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/query"
)

func TestQueryExemplar(t *testing.T) {
	tcs := []struct {
		name       string
		start      string
		end        string
		timeout    string
		query      string
		statusCode int
		shouldErr  bool
		err        string
	}{
		{
			name:       "normal_unix",
			start:      "1617694947",
			end:        "1625557347",
			query:      "metric_name",
			statusCode: http.StatusOK,
		},
		{
			name:       "normal_unix_binary_expr",
			start:      "1617694947",
			end:        "1625557347",
			query:      "a - b",
			statusCode: http.StatusOK,
		},
		{
			name:       "normal_rfc3339",
			start:      "2021-04-06T07:42:27.000Z",
			end:        "2021-07-06T07:42:27.000Z",
			query:      "metric_name",
			statusCode: http.StatusOK,
		},
		{
			name:       "normal_empty_query",
			start:      "1617694947",
			end:        "1625557347",
			query:      "metric_name",
			statusCode: http.StatusOK,
		},
		{
			name:       "empty_start",
			start:      "",
			end:        "1625557347",
			query:      "metric_name",
			statusCode: http.StatusBadRequest,
			shouldErr:  true,
			err:        `{"status":"error","errorType":"bad_data","error":"cannot parse \"\" to a valid timestamp"}`,
		},
		{
			name:       "empty_end",
			start:      "1617694947",
			end:        "",
			query:      "metric_name",
			statusCode: http.StatusBadRequest,
			shouldErr:  true,
			err:        `{"status":"error","errorType":"bad_data","error":"cannot parse \"\" to a valid timestamp"}`,
		},
		{
			name:       "start_greater_than_end",
			start:      "1625557347",
			end:        "1617694947",
			query:      "metric_name",
			statusCode: http.StatusBadRequest,
			shouldErr:  true,
			err:        `{"status":"error","errorType":"bad_data","error":"end timestamp must not be before start time"}`,
		},
		{
			name:       "parser_error",
			start:      "1617694947",
			end:        "1625557347",
			query:      `metric_name{job~="some_value.*"}`,
			statusCode: http.StatusBadRequest,
			shouldErr:  true,
			err:        `{"status":"error","errorType":"bad_data","error":"1:16: parse error: unexpected character inside braces: '~'"}`,
		},
	}

	queryable := query.NewQueryable(mockQuerier{}, nil)
	receivedQueriesCounter := &mockMetric{}
	failedQueriesCounter := &mockMetric{}
	invalidQueryReqs := &mockMetric{}
	queryDuration := &mockMetric{}
	metrics := &Metrics{
		FailedQueries:    failedQueriesCounter,
		ReceivedQueries:  receivedQueriesCounter,
		InvalidQueryReqs: invalidQueryReqs,
		QueryDuration:    queryDuration,
	}

	for _, tc := range tcs {
		handler := queryExemplar(queryable, metrics)
		preparedURL := constructQueryExemplarRequest(tc.query, tc.start, tc.end, tc.timeout)
		r := doExemplarQuery(t, "GET", preparedURL, handler)
		require.Equal(t, tc.statusCode, r.Code, fmt.Sprintf("received code %d, expected %d", r.Code, tc.statusCode), tc.name)
		if tc.shouldErr {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, tc.err, string(b[:len(b)-1]), tc.name) // string(b[:len(b)-1]) to skip the ending \n
		}
	}
}

func constructQueryExemplarRequest(query, start, end, timeout string) string {
	return fmt.Sprintf("http://localhost:9090/query_exemplars?query=%s&start=%s&end=%s&timeout=%s",
		query, start, end, timeout,
	)
}

func doExemplarQuery(t *testing.T, method, url string, handler http.Handler) *httptest.ResponseRecorder {
	req, err := http.NewRequest(method, url, nil)
	require.NoError(t, err)
	req.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded; param=value",
	)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}
