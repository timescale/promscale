// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package integration_tests

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

const matcherProgress = `[{__name__ progress_metric {} [] 0} {job ci-migration {} [] 0}]`

type remoteWriteServer struct {
	mux                    sync.RWMutex
	writeServer            *httptest.Server
	progressServer         *httptest.Server
	writeStorageTimeSeries map[string]prompb.TimeSeries
}

var serverMintReceived, serverMaxtReceived int64

// createRemoteWriteServer creates a write server that exposes a /write endpoint for ingesting samples. It returns a server
// which is expected to be closed by the caller.
func createRemoteWriteServer(t *testing.T, respectTimeOrder bool, testRetry bool) (*remoteWriteServer, string, string) {
	if respectTimeOrder {
		atomic.StoreInt64(&serverMintReceived, math.MaxInt64)
		atomic.StoreInt64(&serverMaxtReceived, math.MinInt64)
	}
	rws := &remoteWriteServer{
		writeStorageTimeSeries: make(map[string]prompb.TimeSeries),
	}
	s := httptest.NewServer(getWriteHandler(t, rws, respectTimeOrder, testRetry))
	spg := httptest.NewServer(getProgressHandler(t, rws, matcherProgress))
	rws.writeServer = s
	rws.progressServer = spg
	return rws, s.URL, spg.URL
}

// Series returns the number of series in the remoteWriteServer.
func (rwss *remoteWriteServer) Series() int {
	rwss.mux.RLock()
	defer rwss.mux.RUnlock()
	count := 0 // To prevent counting as series if no samples occur.
	for _, s := range rwss.writeStorageTimeSeries {
		if len(s.Samples) > 0 {
			count++
		}
	}
	return count
}

// Samples returns the number of samples in the remoteWriteServer.
func (rwss *remoteWriteServer) Samples() int {
	numSamples := 0
	rwss.mux.RLock()
	defer rwss.mux.RUnlock()
	for _, s := range rwss.writeStorageTimeSeries {
		numSamples += len(s.Samples)
	}
	return numSamples
}

func (rwss *remoteWriteServer) AreReceivedSamplesOrdered() bool {
	rwss.mux.RLock()
	defer rwss.mux.RUnlock()
	for _, ts := range rwss.writeStorageTimeSeries {
		if !areSamplesSorted(ts.Samples) {
			return false
		}
	}
	return true
}

// Samples returns the number of samples in the remoteWriteServer.
func (rwss *remoteWriteServer) SamplesProgress() int {
	rwss.mux.RLock()
	defer rwss.mux.RUnlock()
	return len(rwss.writeStorageTimeSeries[matcherProgress].Samples)
}

// Close closes the server(s).
func (rwss *remoteWriteServer) Close() {
	rwss.writeServer.Close()
	rwss.progressServer.Close()
}

var writeRequestCount int32

func getWriteHandler(t *testing.T, rws *remoteWriteServer, respectTimeOrder bool, testRetry bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// we treat invalid requests as the same as no request for
		// leadership-timeout purposes
		if !validateWriteHeaders(t, r) {
			t.Fatal("could not validate write headers")
		}

		reqBuf, err, msg := decodeSnappyBody(r.Body)
		if err != nil {
			t.Fatal("msg", msg, "err", err.Error())
		}

		if testRetry {
			atomic.AddInt32(&writeRequestCount, 1)
			if atomic.LoadInt32(&writeRequestCount) >= 50 {
				if atomic.LoadInt32(&writeRequestCount) >= 55 {
					// Continue to fail for 5 more consecutive requests in order to test max-retries.
					atomic.StoreInt32(&writeRequestCount, 0)
				}
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("known interruption"))
				t.Logf("writer: sending error to verify retrying behaviour")
				return
			}
		}

		req := NewWriteRequest()
		err = proto.Unmarshal(reqBuf, req)
		assert.NoError(t, err)

		rws.mux.Lock()
		defer rws.mux.Unlock()
		if len(req.Timeseries) > 0 {
			if respectTimeOrder {
				if !(atomic.LoadInt64(&serverMintReceived) == math.MaxInt64 && atomic.LoadInt64(&serverMaxtReceived) == math.MinInt64) {
					mint, maxt := getTsMintMaxt(req.Timeseries)
					if l := atomic.LoadInt64(&serverMintReceived); mint < l {
						t.Fatalf("time order not respected: received mint %d, less than global mint %d", mint, l)
					}
					if l := atomic.LoadInt64(&serverMaxtReceived); maxt < l {
						t.Fatalf("time order not respected: received maxt %d, less than global maxt %d", maxt, l)
					}
				}
				currentBatchMint, currentBatchMaxt := getTsMintMaxt(req.Timeseries)
				atomic.StoreInt64(&serverMintReceived, currentBatchMint)
				atomic.StoreInt64(&serverMaxtReceived, currentBatchMaxt)
			}
			for _, ts := range req.Timeseries {
				if respectTimeOrder && !areSamplesSorted(ts.Samples) {
					t.Fatal("received samples are not sorted in ascending order with respect to time")
				}
				m, ok := rws.writeStorageTimeSeries[fmt.Sprintf("%v", ts.Labels)]
				if !ok {
					rws.writeStorageTimeSeries[fmt.Sprintf("%v", ts.Labels)] = ts
					continue
				}
				m.Samples = append(m.Samples, ts.Samples...)
				rws.writeStorageTimeSeries[fmt.Sprintf("%v", ts.Labels)] = m
			}
		}
		FinishWriteRequest(req)
	})
}

func areSamplesSorted(s []prompb.Sample) bool {
	for i := 0; i < len(s)-1; i++ {
		if s[i].Timestamp > s[i+1].Timestamp {
			return false
		}
	}
	return true
}

func getTsMintMaxt(ts []prompb.TimeSeries) (mint, maxt int64) {
	mint = math.MaxInt64
	maxt = math.MinInt64

	for _, t := range ts {
		for _, sample := range t.Samples {
			timestamp := sample.Timestamp
			if sample.Timestamp < mint {
				mint = timestamp
			}
			if sample.Timestamp > maxt {
				maxt = timestamp
			}
		}
	}
	return
}

func getProgressHandler(t *testing.T, rws *remoteWriteServer, labels string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validateReadHeaders(t, w, r) {
			t.Fatal("invalid read headers")
		}

		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal("msg", "read header validation error", "err", err.Error())
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			t.Fatal("msg", "snappy decode error", "err", err.Error())
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			t.Fatal("msg", "proto unmarshal error", "err", err.Error())
		}
		resp := &prompb.ReadResponse{
			Results: make([]*prompb.QueryResult, len(req.Queries)),
		}

		// Since the matchers would be ""=~.* which means that all available matchers.
		// In order to avoid creating another reader and then facing the dupicate enum issue due to
		// importing promscale.prompb and prometheus.prompb, we just listen to the start and end
		// timestamps only.
		startTs := req.Queries[0].StartTimestampMs
		endTs := req.Queries[0].EndTimestampMs
		ts := make([]*prompb.TimeSeries, 1) // Since the response is going to be the number of time-series.
		ts[0] = new(prompb.TimeSeries)
		serie, ok := rws.writeStorageTimeSeries[labels]
		if ok {
			for _, s := range serie.Samples {
				if s.Timestamp >= startTs && s.Timestamp <= endTs {
					ts[0].Samples = append(ts[0].Samples, s)
				}
			}
		}
		ts[0].Labels = serie.Labels
		if len(resp.Results) == 0 {
			t.Fatal("queries num is 0")
		}
		resp.Results[0] = &prompb.QueryResult{Timeseries: ts}
		data, err := proto.Marshal(resp)
		if err != nil {
			t.Fatal("msg", "internal server error", "err", err.Error())
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			t.Fatal("msg", "snappy encode: internal server error", "err", err.Error())
		}
	})
}

var wrPool = sync.Pool{
	New: func() interface{} {
		return new(prompb.WriteRequest)
	},
}

func NewWriteRequest() *prompb.WriteRequest {
	return wrPool.Get().(*prompb.WriteRequest)
}

func FinishWriteRequest(wr *prompb.WriteRequest) {
	for i := range wr.Timeseries {
		ts := &wr.Timeseries[i]
		for j := range ts.Labels {
			ts.Labels[j] = prompb.Label{}
		}
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
		ts.XXX_unrecognized = nil
	}
	wr.Timeseries = wr.Timeseries[:0]
	wr.XXX_unrecognized = nil
	wrPool.Put(wr)
}

func decodeSnappyBody(r io.Reader) ([]byte, error, string) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err, "Read error"
	}

	buf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err, "Snappy decode error"
	}

	return buf, nil, ""
}

func validateWriteHeaders(t *testing.T, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	assert.Equal(t, "POST", r.Method)

	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		assert.Contains(t, "snappy", r.Header.Get("Content-Encoding"))

		remoteWriteVersion := r.Header.Get("X-Prometheus-Remote-Write-Version")
		if assert.NotEmpty(t, remoteWriteVersion) {
			assert.True(
				t,
				strings.HasPrefix(remoteWriteVersion, "0.1."),
				"unexpected Remote-Write-Version %s, expected 0.1.X",
				remoteWriteVersion,
			)
		}
	case "application/json":
		// Don't need any other header checks for JSON content type.
	default:
		t.Fatal("unsupported data format (not protobuf or json)")
	}

	assert.Equal(t, "custom-header-value", r.Header.Get("Custom-Header-Single"))
	assert.Equal(t, []string{"multiple-1", "multiple-2"}, r.Header.Values("Custom-Header-Multiple"))

	return true
}
