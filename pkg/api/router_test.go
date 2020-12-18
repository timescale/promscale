// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type mockHTTPHandler struct {
	w http.ResponseWriter
	r *http.Request
}

func (m *mockHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.w = w
	m.r = r
}

type mockObserverVec struct {
	labelValues []string
	o           prometheus.Observer
}

func (m *mockObserverVec) GetMetricWith(_ prometheus.Labels) (prometheus.Observer, error) {
	panic("not implemented")
}

func (m *mockObserverVec) GetMetricWithLabelValues(lvs ...string) (prometheus.Observer, error) {
	panic("not implemented")
}

func (m *mockObserverVec) With(_ prometheus.Labels) prometheus.Observer {
	panic("not implemented")
}

func (m *mockObserverVec) WithLabelValues(l ...string) prometheus.Observer {
	m.labelValues = l
	return m.o
}

func (m *mockObserverVec) CurryWith(_ prometheus.Labels) (prometheus.ObserverVec, error) {
	panic("not implemented")
}

func (m *mockObserverVec) MustCurryWith(_ prometheus.Labels) prometheus.ObserverVec {
	panic("not implemented")
}

func (m *mockObserverVec) Describe(_ chan<- *prometheus.Desc) {
	panic("not implemented")
}

func (m *mockObserverVec) Collect(_ chan<- prometheus.Metric) {
	panic("not implemented")
}

type mockObserver struct {
	values []float64
}

func (m *mockObserver) Observe(v float64) {
	m.values = append(m.values, v)
}

func TestTimeHandler(t *testing.T) {
	mockObs := &mockObserver{}
	mockObserverVec := &mockObserverVec{
		o: mockObs,
	}

	mockHandler := &mockHTTPHandler{}

	path := "testpath"

	handler := timeHandler(mockObserverVec, path, mockHandler)

	test := generateHandleTester(t, handler)

	test("GET", strings.NewReader(""))

	if len(mockObs.values) != 1 {
		t.Errorf("Did not observe elapsed time from request")
	}

	if mockHandler.r == nil {
		t.Errorf("Did not call HTTP handler")
	}
}

func generateHandleTester(t *testing.T, handleFunc http.Handler) HandleTester {
	return func(method string, body io.Reader) *httptest.ResponseRecorder {
		req, err := http.NewRequest(method, "", body)
		if err != nil {
			t.Errorf("%v", err)
		}
		req.Header.Set(
			"Content-Type",
			"application/x-www-form-urlencoded; param=value",
		)
		w := httptest.NewRecorder()
		handleFunc.ServeHTTP(w, req)
		return w
	}
}

func TestAuthHandler(t *testing.T) {
	testCases := []struct {
		name       string
		cfg        *Config
		headers    map[string]string
		authorized bool
	}{
		{
			name:       "no auth",
			cfg:        &Config{},
			authorized: true,
		},
		{
			name: "no auth header",
			cfg: &Config{
				Auth: &Auth{
					BasicAuthUsername: "foo",
				},
			},
		},
		{
			name: "wrong auth header",
			cfg: &Config{
				Auth: &Auth{
					BasicAuthUsername: "foo",
					BasicAuthPassword: "bar",
				},
			},
			headers: map[string]string{
				"Authorization": "wrong",
			},
		},
		{
			name: "correct auth header",
			cfg: &Config{
				Auth: &Auth{
					BasicAuthUsername: "foo",
					BasicAuthPassword: "bar",
				},
			},
			headers: map[string]string{
				"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("foo:bar")),
			},
			authorized: true,
		},
		{
			name: "wrong bearer token",
			cfg: &Config{
				Auth: &Auth{
					BearerToken: "foo",
				},
			},
			headers: map[string]string{
				"Authorization": "Bearer bar",
			},
		},
		{
			name: "correct bearer token",
			cfg: &Config{
				Auth: &Auth{
					BearerToken: "foo",
				},
			},
			headers: map[string]string{
				"Authorization": "Bearer foo",
			},
			authorized: true,
		},
	}

	handler := func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "", http.StatusOK)
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			w := httptest.NewRecorder()

			req, err := http.NewRequest("GET", "", nil)
			if err != nil {
				t.Errorf("%v", err)
			}

			for name, value := range c.headers {
				req.Header.Set(name, value)
			}

			h := authHandler(c.cfg, handler)
			h.ServeHTTP(w, req)

			if c.authorized && w.Code != http.StatusOK {
				t.Errorf("request should be authorized, was not: %d", w.Code)

			}
			if !c.authorized && w.Code == http.StatusOK {
				t.Errorf("request should not be authorized")
			}
		})
	}
}
