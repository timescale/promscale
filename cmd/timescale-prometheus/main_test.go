package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/timescale/timescale-prometheus/pkg/prompb"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgclient"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

var (
	healthOKHeaderMap = http.Header{
		"Content-Length": []string{"0"},
	}
)

type mockHealthChecker struct {
	returnErr error
}

func (m *mockHealthChecker) HealthCheck() error {
	return m.returnErr
}

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

type mockReader struct {
	request  *prompb.ReadRequest
	response *prompb.ReadResponse
	err      error
}

func (m *mockReader) Read(r *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	m.request = r
	return m.response, m.err
}

type mockInserter struct {
	ts     []prompb.TimeSeries
	result uint64
	err    error
}

func (m *mockInserter) Ingest(ts []prompb.TimeSeries) (uint64, error) {
	m.ts = ts
	return m.result, m.err
}

type mockElection struct {
	isLeader bool
	err      error
}

func (m *mockElection) ID() string {
	return "ID"
}

func (m *mockElection) BecomeLeader() (bool, error) {
	return true, nil
}

func (m *mockElection) IsLeader() (bool, error) {
	return m.isLeader, m.err
}

func (m *mockElection) Resign() error {
	return nil
}

type mockGauge struct {
	value float64
}

func (m *mockGauge) Desc() *prometheus.Desc {
	panic("not implemented")
}

func (m *mockGauge) Write(_ *dto.Metric) error {
	panic("not implemented")
}

func (m *mockGauge) Describe(_ chan<- *prometheus.Desc) {
	panic("not implemented")
}

func (m *mockGauge) Collect(_ chan<- prometheus.Metric) {
	panic("not implemented")
}

func (m *mockGauge) Set(v float64) {
	m.value = v
}

func (m *mockGauge) Inc() {
	panic("not implemented")
}

func (m *mockGauge) Dec() {
	panic("not implemented")
}

func (m *mockGauge) Add(_ float64) {
	panic("not implemented")
}

func (m *mockGauge) Sub(_ float64) {
	panic("not implemented")
}

func (m *mockGauge) SetToCurrentTime() {
	panic("not implemented")
}

func TestMain(m *testing.M) {
	flag.Parse()
	err := log.Init("debug")

	if err != nil {
		fmt.Println("Error initializing logger", err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func TestHealth(t *testing.T) {
	testCases := []struct {
		name                   string
		httpStatus             int
		healthCheckerReturnErr error
	}{
		{
			name:       "no error",
			httpStatus: http.StatusOK,
		},
		{
			name:                   "error",
			httpStatus:             http.StatusInternalServerError,
			healthCheckerReturnErr: fmt.Errorf("some error"),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mock := &mockHealthChecker{
				returnErr: c.healthCheckerReturnErr,
			}

			healthHandle := health(mock)

			test := GenerateHandleTester(t, healthHandle)
			w := test("GET", strings.NewReader(""))

			if w.Code != c.httpStatus {
				t.Errorf("Health page didn't return correct status: got %v wanted %v", w.Code, c.httpStatus)
			}

			header := w.Header()

			if c.httpStatus == http.StatusOK {
				if !reflect.DeepEqual(header, healthOKHeaderMap) {
					t.Errorf("Did not get correct headers for http.StatusOK:\ngot\n%#v\nwanted\n%#v\n", header, healthOKHeaderMap)
				}
			} else {
				if strings.TrimSpace(w.Body.String()) != c.healthCheckerReturnErr.Error() {
					t.Errorf("Unexpected body content:\ngot\n%s\nwanted\n%s", w.Body.String(), c.healthCheckerReturnErr.Error())
				}
			}

		})
	}
}

func TestTimeHandler(t *testing.T) {
	mockObs := &mockObserver{}
	mockObserverVec := &mockObserverVec{
		o: mockObs,
	}

	mockHandler := &mockHTTPHandler{}

	path := "testpath"

	handler := timeHandler(mockObserverVec, path, mockHandler)

	test := GenerateHandleTester(t, handler)

	test("GET", strings.NewReader(""))

	if len(mockObs.values) != 1 {
		t.Errorf("Did not observe elapsed time from request")
	}

	if mockHandler.r == nil {
		t.Errorf("Did not call HTTP handler")
	}
}

func readRequestToString(r *prompb.ReadRequest) string {
	data, _ := proto.Marshal(r)
	return string(snappy.Encode(nil, data))
}

func writeRequestToString(r *prompb.WriteRequest) string {
	data, _ := proto.Marshal(r)
	return string(snappy.Encode(nil, data))
}

func getReader(s string) io.Reader {
	var r io.Reader = strings.NewReader(s)
	if s == "" {
		r = iotest.TimeoutReader(r)
		_, _ = r.Read([]byte{})
	}
	return r
}

func TestRead(t *testing.T) {
	testCases := []struct {
		name           string
		responseCode   int
		requestBody    string
		readerResponse *prompb.ReadResponse
		readerErr      error
	}{
		{
			name:         "read request body error",
			responseCode: http.StatusInternalServerError,
		},
		{
			name:         "malformed compression data",
			responseCode: http.StatusBadRequest,
			requestBody:  "123",
		},
		{
			name:         "malformed read request",
			responseCode: http.StatusBadRequest,
			requestBody:  string(snappy.Encode(nil, []byte("test"))),
		},
		{
			name:         "reader error",
			responseCode: http.StatusInternalServerError,
			readerErr:    fmt.Errorf("some error"),
			requestBody: readRequestToString(
				&prompb.ReadRequest{},
			),
		},
		{
			name:           "happy path",
			responseCode:   http.StatusOK,
			readerResponse: &prompb.ReadResponse{},
			requestBody: readRequestToString(
				&prompb.ReadRequest{},
			),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			mockReader := &mockReader{
				response: c.readerResponse,
				err:      c.readerErr,
			}

			handler := read(mockReader)

			test := GenerateHandleTester(t, handler)

			w := test("GET", getReader(c.requestBody))

			if w.Code != c.responseCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, c.responseCode)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	testCases := []struct {
		name             string
		responseCode     int
		requestBody      string
		inserterResponse uint64
		inserterErr      error
		isLeader         bool
		electionErr      error
	}{
		{
			name:         "write request body error",
			isLeader:     true,
			responseCode: http.StatusInternalServerError,
		},
		{
			name:         "malformed compression data",
			isLeader:     true,
			responseCode: http.StatusBadRequest,
			requestBody:  "123",
		},
		{
			name:         "malformed write request",
			isLeader:     true,
			responseCode: http.StatusBadRequest,
			requestBody:  string(snappy.Encode(nil, []byte("test"))),
		},
		{
			name:         "write error",
			isLeader:     true,
			responseCode: http.StatusInternalServerError,
			inserterErr:  fmt.Errorf("some error"),
			requestBody: writeRequestToString(
				&prompb.WriteRequest{},
			),
		},
		{
			name:         "elector error",
			electionErr:  fmt.Errorf("some error"),
			responseCode: http.StatusOK,
		},
		{
			name:         "not a leader",
			responseCode: http.StatusOK,
		},
		{
			name:             "happy path",
			isLeader:         true,
			responseCode:     http.StatusOK,
			inserterResponse: 3,
			requestBody: writeRequestToString(
				&prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{},
					},
				},
			),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			elector = util.NewElector(
				&mockElection{
					isLeader: c.isLeader,
					err:      c.electionErr,
				},
			)
			mockGauge := &mockGauge{}
			leaderGauge = mockGauge
			mock := &mockInserter{
				result: c.inserterResponse,
				err:    c.inserterErr,
			}

			handler := write(mock)

			test := GenerateHandleTester(t, handler)

			w := test("GET", getReader(c.requestBody))

			if w.Code != c.responseCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, c.responseCode)
			}

			if c.electionErr != nil && mockGauge.value != 0 {
				t.Errorf("leader gauge metric not set correctly: got %f when election returns an error", mockGauge.value)
			}

			switch {
			case c.isLeader && mockGauge.value != 1:
				t.Errorf("leader gauge metric not set correctly: got %f when is leader", mockGauge.value)
			case !c.isLeader && mockGauge.value != 0:
				t.Errorf("leader gauge metric not set correctly: got %f when is not leader", mockGauge.value)
			}
		})
	}
}

func TestInitElector(t *testing.T) {
	// TODO: refactor the function to be fully testable without using a DB.
	testCases := []struct {
		name         string
		cfg          *config
		shouldError  bool
		electionType reflect.Type
	}{
		{
			name: "Cannot create REST election with a group lock ID",
			cfg: &config{
				haGroupLockID: 1,
				restElection:  true,
			},
			shouldError: true,
		},
		{
			name: "Create REST elector",
			cfg: &config{
				haGroupLockID: 0,
				restElection:  true,
			},
			electionType: reflect.TypeOf(&util.RestElection{}),
		},
		{
			name: "Cannot create scheduled elector, no group lock ID and not rest election",
			cfg: &config{
				haGroupLockID: 0,
			},
		},
		{
			name: "Prometheus timeout not set for PG advisory lock",
			cfg: &config{
				haGroupLockID:     1,
				prometheusTimeout: -1,
			},
			shouldError: true,
		},
		{
			name: "Can't get advisory lock, couldn't connect to DB",
			cfg: &config{
				haGroupLockID:     1,
				prometheusTimeout: 0,
			},
			shouldError: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			elector, err := initElector(c.cfg)

			switch {
			case err != nil && !c.shouldError:
				t.Errorf("Unexpected error, got %s", err.Error())
			case err == nil && c.shouldError:
				t.Errorf("Expected error, got nil")
			}

			if c.electionType != nil {
				if elector == nil {
					t.Fatalf("Expected to create elector, got nil")
				}

				v := reflect.ValueOf(elector).Elem().Field(0).Elem()

				if v.Type() != c.electionType {
					t.Errorf("Wrong type of elector created: got %v wanted %v", v.Type(), c.electionType)
				}
			}
		})
	}
}

func TestMigrate(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *pgclient.Config
		isLeader    bool
		electionErr error
		shouldError bool
	}{
		{
			name:        "elector error",
			electionErr: fmt.Errorf("some error"),
			shouldError: true,
		},
		{
			name: "not a leader",
		},
		{
			name:        "is leader",
			isLeader:    true,
			cfg:         &pgclient.Config{},
			shouldError: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			elector = util.NewElector(
				&mockElection{
					isLeader: c.isLeader,
					err:      c.electionErr,
				},
			)
			mockGauge := &mockGauge{}
			leaderGauge = mockGauge

			err := migrate(c.cfg)

			switch {
			case err != nil && !c.shouldError:
				t.Errorf("Unexpected error returned:\ngot\n%s\nwanted nil\n", err)
			case err == nil && c.shouldError:
				t.Errorf("Expected error to be returned: got nil")
			}

			switch {
			case c.isLeader && mockGauge.value != 1:
				t.Errorf("Leader gauge metric not set correctly: got %f when is leader", mockGauge.value)
			case !c.isLeader && mockGauge.value != 0:
				t.Errorf("Leader gauge metric not set correctly: got %f when is not leader", mockGauge.value)
			}
		})
	}
}

type HandleTester func(method string, body io.Reader) *httptest.ResponseRecorder

func GenerateHandleTester(t *testing.T, handleFunc http.Handler) HandleTester {
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
