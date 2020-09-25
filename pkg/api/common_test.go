package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"testing"

	"github.com/timescale/promscale/pkg/log"
)

func TestCORSWrapper(t *testing.T) {
	_ = log.Init(log.Config{
		Level: "debug",
	})
	acceptSpecific, _ := regexp.Compile("^(?:" + "http://some-site.com" + ")$")
	acceptAny, _ := regexp.Compile("^(?:" + ".*" + ")$")

	testCases := []struct {
		name           string
		requestOrigin  string
		acceptedOrigin *regexp.Regexp
		expectHeaders  map[string][]string
	}{
		{
			name:           "No origin",
			requestOrigin:  "",
			acceptedOrigin: acceptSpecific,
			expectHeaders:  map[string][]string{},
		}, {
			name:           "Origin doesn't match accepted",
			requestOrigin:  "http://some-unknown-site.com",
			acceptedOrigin: acceptSpecific,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Vary":                          {"Origin"},
			},
		},
		{
			name:           "Origin matches accepted",
			requestOrigin:  "http://some-site.com",
			acceptedOrigin: acceptSpecific,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Access-Control-Allow-Origin":   {"http://some-site.com"},
				"Vary":                          {"Origin"},
			},
		}, {
			name:           "Wildcard allowed origin",
			requestOrigin:  "http://any-site.com",
			acceptedOrigin: acceptAny,
			expectHeaders: map[string][]string{
				"Access-Control-Allow-Headers":  {"Accept, Authorization, Content-Type, Origin"},
				"Access-Control-Allow-Methods":  {"GET, POST, OPTIONS"},
				"Access-Control-Expose-Headers": {"Date"},
				"Access-Control-Allow-Origin":   {"*"},
				"Vary":                          {"Origin"},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := &Config{}
			if tc.acceptedOrigin != nil {
				conf.AllowedOrigin = tc.acceptedOrigin
			} else {
				tc.acceptedOrigin = &regexp.Regexp{}
			}
			internalHandlerCalled := false
			handler := corsWrapper(conf, func(http.ResponseWriter, *http.Request) {
				internalHandlerCalled = true
			})
			w := doCORSWrapperRequest(t, handler, "http://localhost/", tc.requestOrigin)
			if !internalHandlerCalled {
				t.Fatalf("internal handler not called by CORS wrapper")
				return
			}
			returnedHeaders := w.Header()
			if len(returnedHeaders) != len(tc.expectHeaders) {
				t.Fatalf("expected %d headers, got %d", len(tc.expectHeaders), len(returnedHeaders))
				return
			}
			for hName, hValues := range tc.expectHeaders {
				returnedValues := returnedHeaders[hName]
				if !reflect.DeepEqual(hValues, returnedValues) {
					t.Errorf("expected header %s with value %v; got %v", hName, hValues, returnedValues)
				}
			}
		})

	}

}

func doCORSWrapperRequest(t *testing.T, queryHandler http.Handler, url, origin string) *httptest.ResponseRecorder {
	req, err := http.NewRequestWithContext(context.Background(), "GET", url, nil)
	if err != nil {
		t.Errorf("%v", err)
	}

	req.Header.Set("Origin", origin)
	w := httptest.NewRecorder()
	queryHandler.ServeHTTP(w, req)
	return w
}
