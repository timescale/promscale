package api

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestImportSampleUnmarshalJSON(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		timestamp   int64
		value       float64
		shouldError bool
	}{
		{
			name:      "happy path",
			input:     "[1000, 2.3]",
			timestamp: 1000,
			value:     2.3,
		},
		{
			name:        "empty JSON",
			input:       "",
			shouldError: true,
		},
		{
			name:        "empty array",
			input:       "[]",
			shouldError: true,
		},
		{
			name:        "missing value",
			input:       "[1000]",
			shouldError: true,
		},
		{
			name:        "more than two input values",
			input:       "[1,2,3]",
			shouldError: true,
		},
		{
			name:        "invalid timestamp",
			input:       `["1",2]`,
			shouldError: true,
		},
		{
			name:        "invalid value",
			input:       `[1,"2"]`,
			shouldError: true,
		},
		{
			name:        "timestamp should not contain decimal place",
			input:       `[10.1,10.2]`,
			shouldError: true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			i := &importSample{}

			err := i.UnmarshalJSON([]byte(c.input))

			if err != nil {
				if !c.shouldError {
					t.Errorf("unexpected error found: %s", err.Error())
				}
				return
			} else {
				if c.shouldError {
					t.Errorf("expected error for test case, found nil")
					return
				}
			}

			if c.timestamp != i.Timestamp {
				t.Errorf("invalid timestamp found, wanted %d, got %d", c.timestamp, i.Timestamp)
			}

			if c.value != i.Value {
				t.Errorf("invalid value found, wanted %f, got %f", c.value, i.Value)
			}
		})

	}
}

func TestAppendImportPayload(t *testing.T) {
	testCases := []struct {
		name   string
		input  importPayload
		result prompb.WriteRequest
	}{
		{
			name:   "empty payload",
			result: prompb.WriteRequest{},
		},
		{
			name: "empty samples",
			input: importPayload{
				Labels: map[string]string{
					"labelName": "labelValue",
				},
			},
			result: prompb.WriteRequest{},
		},
		{
			name: "empty labels",
			input: importPayload{
				Samples: []importSample{
					{
						Timestamp: 1,
						Value:     2,
					},
				},
			},
			result: prompb.WriteRequest{},
		},
		{
			name: "happy path",
			input: importPayload{
				Labels: map[string]string{
					"labelName": "labelValue",
				},
				Samples: []importSample{
					{
						Timestamp: 1,
						Value:     2.3,
					},
				},
			},
			result: prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					prompb.TimeSeries{
						Labels: []prompb.Label{
							prompb.Label{
								Name:  "labelName",
								Value: "labelValue",
							},
						},
						Samples: []prompb.Sample{
							prompb.Sample{
								Timestamp: 1,
								Value:     2.3,
							},
						},
					},
				},
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			wr := &prompb.WriteRequest{}
			got := appendImportPayload(c.input, wr)

			if !reflect.DeepEqual(c.result, *got) {
				t.Errorf("unexpected result\ngot:\n%v\nwanted:\n%v\n", *got, c.result)

			}
		})
	}

}

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
	testutil.Ok(t, log.Init(log.Config{
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
		inserterResponse uint64
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
						{},
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

			headers := protobufHeaders
			if len(c.customHeaders) != 0 {
				headers = c.customHeaders

			}

			test := GenerateWriteHandleTester(t, handler, headers)

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
