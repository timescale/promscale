package api

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestDelete(t *testing.T) {
	cases := []struct {
		name         string
		matchers     []string
		start        string
		end          string
		fails        bool
		message      string
		expectedCode int
	}{
		{
			name:         "normal_single_matcher",
			matchers:     []string{`{__name__=~".*"}`},
			expectedCode: http.StatusOK,
		},
		{
			name:         "normal_multiple_matchers",
			matchers:     []string{`{__name__=~".*"}`, `{__name__="go_goroutines", job="prometheus"}`},
			expectedCode: http.StatusOK,
		},
		{
			name:         "empty_matchers",
			matchers:     []string{},
			expectedCode: http.StatusBadRequest,
			fails:        true,
			message:      "no match[] parameter provided",
		},
		{
			name:         "normal_with_start",
			matchers:     []string{`{__name__=~".*"}`},
			start:        "1604311719000",
			expectedCode: http.StatusBadRequest,
			fails:        true,
			message:      "time based series deletion is unsupported",
		},
		{
			name:         "normal_with_end",
			matchers:     []string{`{__name__=~".*"}`},
			end:          "1604311719000",
			expectedCode: http.StatusBadRequest,
			fails:        true,
			message:      "time based series deletion is unsupported",
		},
		{
			name:         "normal_with_start_end",
			matchers:     []string{`{__name__=~".*"}`},
			start:        "1604311711000",
			end:          "1604311719000",
			expectedCode: http.StatusBadRequest,
			fails:        true,
			message:      "time based series deletion is unsupported",
		},
		{
			name:         "normal_with_start_end_without_matchers",
			matchers:     []string{},
			start:        "1604311711000",
			end:          "1604311719000",
			expectedCode: http.StatusBadRequest,
			fails:        true,
			message:      "no match[] parameter provided",
		},
	}

	config := &Config{
		ReadOnly:        false,
		AdminAPIEnabled: true,
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := deleteHandler(config, nil)
			vals := constructRequestValues(tc.start, tc.end, tc.matchers)
			// Post delete request.
			wPost := doPostDeleteRequest(t, handler, vals)
			if wPost.StatusCode != tc.expectedCode {
				t.Errorf("post: Unexpected HTTP status code received: got %d wanted %d", wPost.StatusCode, tc.expectedCode)
				return
			}
			if tc.fails {
				bstream, err := ioutil.ReadAll(wPost.Body)
				if err != nil {
					t.Fatal(err)
				}
				var errMessage errResponse
				if err = json.Unmarshal(bstream, &errMessage); err != nil {
					t.Fatal(err)
				}
				if errMessage.Error != tc.message {
					t.Errorf("post: Unexpected error message received: got %s wanted %s", errMessage.Error, tc.message)
					return
				}
			}
			// Put delete request.
			wPut := doPutDeleteRequest(t, handler, vals)
			if wPut.StatusCode != tc.expectedCode {
				t.Errorf("put: Unexpected HTTP status code received: got %d wanted %d", wPut.StatusCode, tc.expectedCode)
				return
			}
			if tc.fails {
				bstream, err := ioutil.ReadAll(wPut.Body)
				if err != nil {
					t.Fatal(err)
				}
				var errMessage errResponse
				if err = json.Unmarshal(bstream, &errMessage); err != nil {
					t.Fatal(err)
				}
				if errMessage.Error != tc.message {
					t.Errorf("put: Unexpected error message received: got %s wanted %s", errMessage.Error, tc.message)
					return
				}
			}
		})

	}
}

func constructRequestValues(start, end string, matchers []string) url.Values {
	values := make(url.Values)
	if start != "" {
		values.Add("start", start)
	}
	if end != "" {
		values.Add("end", end)
	}
	for _, m := range matchers {
		values.Add("match[]", m)
	}
	return values
}

func doPostDeleteRequest(t *testing.T, queryHandler http.Handler, vals url.Values) *http.Response {
	server := httptest.NewServer(queryHandler)
	response, err := http.PostForm(server.URL, vals)
	if err != nil {
		t.Errorf("%v", err)
	}
	server.Close()
	return response
}

func doPutDeleteRequest(t *testing.T, queryHandler http.Handler, vals url.Values) *http.Response {
	server := httptest.NewServer(queryHandler)
	response, err := http.PostForm(server.URL, vals)
	if err != nil {
		t.Errorf("%v", err)
	}
	server.Close()
	return response
}
