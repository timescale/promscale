package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/timescale/promscale/pkg/log"

	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/util"
)

func TestWrite(t *testing.T) {
	testutil.Ok(t, log.Init(log.Config{
		Level: "debug",
	}))
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
			name:         "bad header",
			isLeader:     false,
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
			elector := util.NewElector(
				&mockElection{
					isLeader: c.isLeader,
					err:      c.electionErr,
				},
			)
			leaderGauge := &mockMetric{}
			receivedSamplesGauge := &mockMetric{}
			failedSamplesGauge := &mockMetric{}
			sentSamplesGauge := &mockMetric{}
			sendBatchHistogram := &mockMetric{}
			invalidWriteReqs := &mockMetric{}
			mock := &mockInserter{
				result: c.inserterResponse,
				err:    c.inserterErr,
			}

			handler := Write(mock, elector, &Metrics{
				LeaderGauge:       leaderGauge,
				ReceivedSamples:   receivedSamplesGauge,
				FailedSamples:     failedSamplesGauge,
				SentSamples:       sentSamplesGauge,
				SentBatchDuration: sendBatchHistogram,
				InvalidWriteReqs:  invalidWriteReqs,
				WriteThroughput:   util.NewThroughputCalc(time.Second),
			})

			test := GenerateWriteHandleTester(t, handler, c.name == "bad header")

			w := test("POST", getReader(c.requestBody))

			if w.Code != c.responseCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, c.responseCode)
			}

			if c.electionErr != nil && leaderGauge.value != 0 {
				t.Errorf("leader gauge metric not set correctly: got %f when election returns an error", leaderGauge.value)
			}

			switch {
			case c.isLeader && leaderGauge.value != 1:
				t.Errorf("leader gauge metric not set correctly: got %f when is leader", leaderGauge.value)
			case !c.isLeader && leaderGauge.value != 0:
				t.Errorf("leader gauge metric not set correctly: got %f when is not leader", leaderGauge.value)
			}

			if sentSamplesGauge.value != float64(c.inserterResponse) {
				t.Errorf(
					"num sent samples gauge not set correctly: got %f, expected %d",
					receivedSamplesGauge.value,
					c.inserterResponse,
				)
			}
		})
	}
}

func writeRequestToString(r *prompb.WriteRequest) string {
	data, _ := proto.Marshal(r)
	return string(snappy.Encode(nil, data))
}

type HandleTester func(method string, body io.Reader) *httptest.ResponseRecorder

func GenerateWriteHandleTester(t *testing.T, handleFunc http.Handler, badHeaders bool) HandleTester {
	return func(method string, body io.Reader) *httptest.ResponseRecorder {
		req, err := http.NewRequest(method, "", body)
		if err != nil {
			t.Errorf("%v", err)
		}
		if !badHeaders {
			req.Header.Add("Content-Encoding", "snappy")
			req.Header.Set("Content-Type", "application/x-protobuf")
			req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		}
		w := httptest.NewRecorder()
		handleFunc.ServeHTTP(w, req)
		return w
	}
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

type mockInserter struct {
	ts     []prompb.TimeSeries
	result uint64
	err    error
}

func (m *mockInserter) Ingest(series []prompb.TimeSeries, request *prompb.WriteRequest) (uint64, error) {
	m.ts = series
	return m.result, m.err
}

func getReader(s string) io.Reader {
	var r io.Reader = strings.NewReader(s)
	if s == "" {
		r = iotest.TimeoutReader(r)
		_, _ = r.Read([]byte{})
	}
	return r
}

type mockMetric struct {
	value float64
}

func (m *mockMetric) Observe(f float64) {
	m.value = f
}

func (m *mockMetric) Desc() *prometheus.Desc {
	panic("implement me")
}

func (m *mockMetric) Write(metric *dto.Metric) error {
	metric.Counter = &io_prometheus_client.Counter{}
	metric.Counter.Value = &m.value
	return nil
}

func (m *mockMetric) Describe(descs chan<- *prometheus.Desc) {
	panic("implement me")
}

func (m *mockMetric) Collect(metrics chan<- prometheus.Metric) {
	panic("implement me")
}

func (m *mockMetric) Set(f float64) {
	m.value = f
}

func (m *mockMetric) Inc() {
	m.value += 1
}

func (m *mockMetric) Dec() {
	panic("implement me")
}

func (m *mockMetric) Add(f float64) {
	m.value += f
}

func (m *mockMetric) Sub(f float64) {
	panic("implement me")
}

func (m *mockMetric) SetToCurrentTime() {
	panic("implement me")
}
