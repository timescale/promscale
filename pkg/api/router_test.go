package api

import (
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
