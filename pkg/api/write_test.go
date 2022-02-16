// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"

	"github.com/timescale/promscale/pkg/api/parser"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/util"
)

func TestDetectSnappyStreamFormat(t *testing.T) {
	testCases := []struct {
		name   string
		input  []byte
		result bool
	}{
		{
			name:   "empty input",
			result: false,
		},
		{
			name:   "invalid input",
			input:  []byte("foo"),
			result: false,
		},
		{
			name:   "block format",
			input:  snappy.Encode(nil, []byte("snappy")),
			result: false,
		},
		{
			name:   "too short stream format",
			input:  []byte(getSnappyStreamEncoded("happy path"))[:9],
			result: false,
		},
		{
			name:   "stream format",
			input:  []byte(getSnappyStreamEncoded("happy path")),
			result: true,
		},
		{
			name:   "invalid contents can still be snappy stream formatted",
			input:  []byte(getSnappyStreamEncoded("happy path"))[:11],
			result: true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			if c.result != detectSnappyStreamFormat(c.input) {
				t.Errorf("invalid snappy stream format detection, result should be %v", c.result)
			}
		})
	}

}

func TestWrite(t *testing.T) {
	require.NoError(t, log.Init(log.Config{
		Level: "debug",
	}))

	protobufHeaders := map[string]string{
		"Content-Encoding":                  "snappy",
		"Content-Type":                      "application/x-protobuf",
		"X-Prometheus-Remote-Write-Version": "0.1.0",
	}
	jsonHeaders := map[string]string{
		"Content-Type": "application/json",
	}
	testCases := []struct {
		name             string
		responseCode     int
		requestBody      string
		inserterResponse int64
		inserterErr      error
		isLeader         bool
		electionErr      error
		customHeaders    map[string]string
	}{
		{
			name:         "write request body error",
			isLeader:     true,
			responseCode: http.StatusBadRequest,
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
			customHeaders: map[string]string{
				"foo": "bar",
			},
		},
		{
			name:             "write error",
			isLeader:         true,
			inserterResponse: 1,
			responseCode:     http.StatusInternalServerError,
			inserterErr:      fmt.Errorf("some error"),
			requestBody: writeRequestToString(
				&prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Samples: []prompb.Sample{
								{},
							},
						},
					},
				},
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
			name:         "bad content type header",
			responseCode: http.StatusBadRequest,
			customHeaders: map[string]string{
				"Content-Type": "foo",
			},
		},
		{
			name:         "bad content encoding header",
			responseCode: http.StatusBadRequest,
			customHeaders: map[string]string{
				"Content-Type": "application/x-protobuf",
			},
		},
		{
			name:         "missing remote write version",
			responseCode: http.StatusBadRequest,
			customHeaders: map[string]string{
				"Content-Type":     "application/x-protobuf",
				"Content-Encoding": "snappy",
			},
		},
		{
			name:         "bad remote write version",
			responseCode: http.StatusBadRequest,
			customHeaders: map[string]string{
				"Content-Type":                      "application/x-protobuf",
				"Content-Encoding":                  "snappy",
				"X-Prometheus-Remote-Write-Version": "0.0.0",
			},
		},
		{
			name:             "happy path",
			isLeader:         true,
			responseCode:     http.StatusOK,
			inserterResponse: 3,
			requestBody: writeRequestToString(
				&prompb.WriteRequest{
					Timeseries: []prompb.TimeSeries{
						{
							Samples: []prompb.Sample{
								{},
							},
						},
					},
				},
			),
		},
		{
			name:          "malformed JSON",
			isLeader:      true,
			responseCode:  http.StatusBadRequest,
			requestBody:   ``,
			customHeaders: jsonHeaders,
		},
		{
			name:             "happy path JSON",
			isLeader:         true,
			responseCode:     http.StatusOK,
			inserterResponse: 3,
			requestBody:      `{"labels":{"labelName":"labelValue"}, "samples":[[1,2],[2,2],[3,2]]}`,
			customHeaders:    jsonHeaders,
		},
		{
			name:         "happy path JSON with snappy (stream format)",
			isLeader:     true,
			responseCode: http.StatusOK,
			requestBody:  getSnappyStreamEncoded(`{"labels":{"labelName":"labelValue"}, "samples":[[1,2],[2,2],[3,2]]}`),
			customHeaders: map[string]string{
				"Content-Type":     "application/json",
				"Content-Encoding": "snappy",
			},
		},
		{
			name:         "happy path JSON with snappy (block format)",
			isLeader:     true,
			responseCode: http.StatusOK,
			requestBody:  string(snappy.Encode(nil, []byte(`{"labels":{"labelName":"labelValue"}, "samples":[[1,2],[2,2],[3,2]]}`))),
			customHeaders: map[string]string{
				"Content-Type":     "application/json",
				"Content-Encoding": "snappy",
			},
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
			mock := &mockInserter{
				result: c.inserterResponse,
				err:    c.inserterErr,
			}
			metrics = &Metrics{LastRequestUnixNano: 0}
			dataParser := parser.NewParser()
			numSamples := &mockMetric{}
			handler := Write(mock, dataParser, elector, mockUpdaterForIngest(&mockMetric{}, nil, numSamples, nil))

			headers := protobufHeaders
			if len(c.customHeaders) != 0 {
				headers = c.customHeaders
			}

			test := GenerateWriteHandleTester(t, handler, headers)

			w := test("POST", getReader(c.requestBody))

			if w.Code != c.responseCode {
				t.Errorf("Unexpected HTTP status code received: got %d wanted %d", w.Code, c.responseCode)
			}

			if numSamples.value != float64(c.inserterResponse) {
				t.Errorf(
					"num sent samples gauge not set correctly: got %v, expected %d",
					numSamples.value,
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

func GenerateWriteHandleTester(t *testing.T, handleFunc http.Handler, headers map[string]string) HandleTester {
	return func(method string, body io.Reader) *httptest.ResponseRecorder {
		req, err := http.NewRequest(method, "", body)
		if err != nil {
			t.Errorf("%v", err)
		}

		for name, value := range headers {
			req.Header.Add(name, value)
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
	result int64
	err    error
}

func (m *mockInserter) IngestTraces(_ context.Context, _ pdata.Traces) error {
	panic("not implemented") // TODO: Implement
}

func (m *mockInserter) Ingest(_ context.Context, r *prompb.WriteRequest) (uint64, uint64, error) {
	m.ts = r.Timeseries
	return uint64(m.result), 0, m.err
}

func getReader(s string) io.Reader {
	var r io.Reader = strings.NewReader(s)
	if s == "" {
		r = iotest.TimeoutReader(r)
		_, _ = r.Read([]byte{})
	}
	return r
}

func getSnappyStreamEncoded(s string) string {
	b := strings.Builder{}

	w := snappy.NewBufferedWriter(&b)

	_, _ = w.Write([]byte(s))

	w.Close()
	return b.String()

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

func (m *mockMetric) Write(metric *io_prometheus_client.Metric) error {
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

func mockUpdaterForIngest(counter, histogram, numSamples, numMetadata *mockMetric) func(code string, duration, numSamples, numMetadata float64) {
	return func(_ string, duration, samples, metadata float64) {
		counter.value++
		applyValueIfMetricNotNil(counter, samples)
		applyValueIfMetricNotNil(histogram, duration)
		applyValueIfMetricNotNil(numSamples, samples)
		applyValueIfMetricNotNil(numMetadata, metadata)
	}
}

func mockUpdaterForQuery(counter, histogram *mockMetric) func(handler, code string, duration float64) {
	return func(_, _ string, duration float64) {
		counter.value++
		applyValueIfMetricNotNil(histogram, duration)
	}
}

func applyValueIfMetricNotNil(metric *mockMetric, value float64) {
	if metric == nil {
		return
	}
	metric.value = value
}
