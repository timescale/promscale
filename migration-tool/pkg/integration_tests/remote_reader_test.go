// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package integration_tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

type remoteReadServer struct {
	server *httptest.Server
	series []prompb.TimeSeries
}

// createRemoteReadServer creates a remote read server. It exposes a single /read endpoint and responds with the
// passed series based on the request to the read endpoint. It returns a server which should be closed after
// being used.
func createRemoteReadServer(t *testing.T, seriesToBeSent []prompb.TimeSeries, testRetry bool) (*remoteReadServer, string) {
	s := httptest.NewServer(getReadHandler(t, seriesToBeSent, testRetry))
	return &remoteReadServer{
		server: s,
		series: seriesToBeSent,
	}, s.URL
}

// Series returns the numbr of series in the remoteReadServer.
func (rrs *remoteReadServer) Series() int {
	// Read storage series are immutable. So, no need for read-locks.
	return len(rrs.series)
}

// Samples returns the total number of samples that the remoteReadServer contains.
func (rrs *remoteReadServer) Samples() int {
	// Read storage series are immutable. So, no need for read-locks.
	numSamples := 0
	for _, s := range rrs.series {
		numSamples += len(s.Samples)
	}
	return numSamples
}

// Close closes the server.
func (rrs *remoteReadServer) Close() {
	rrs.server.Close()
}

var readRequestCount int32

func getReadHandler(t *testing.T, series []prompb.TimeSeries, testRetry bool) http.Handler {
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

		if testRetry {
			atomic.AddInt32(&readRequestCount, 1)
			if atomic.LoadInt32(&readRequestCount) >= 50 {
				if atomic.LoadInt32(&readRequestCount) >= 55 {
					// Continue to fail for 5 more consecutive requests in order to test max-retries.
					atomic.StoreInt32(&readRequestCount, 0)
				}
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("known interruption"))
				t.Logf("reader: sending error to verify retrying behaviour")
				return
			}
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
		ts := make([]*prompb.TimeSeries, len(series)) // Since the response is going to be the number of time-series.
		for i, s := range series {
			serie := &prompb.TimeSeries{}
			var samples []prompb.Sample
			for _, sample := range s.Samples {
				if sample.Timestamp >= startTs && sample.Timestamp < endTs {
					// Considering including of time boundaries. Prometheus excludes the end boundary.
					// TODO: check this with the Brian's comments in design doc.
					samples = append(samples, sample)
				}
			}
			if len(samples) > 0 {
				serie.Labels = s.Labels
				serie.Samples = samples
			}
			ts[i] = serie

		}
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
		w.WriteHeader(http.StatusOK)

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			t.Fatal("msg", "snappy encode: internal server error", "err", err.Error())
		}
	})
}

func validateReadHeaders(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	assert.Equal(t, "POST", r.Method)
	assert.Equal(t, "snappy", r.Header.Get("Content-Encoding"))
	assert.Equal(t, "application/x-protobuf", r.Header.Get("Content-Type"))

	remoteReadVersion := r.Header.Get("X-Prometheus-Remote-Read-Version")
	if assert.NotEmpty(t, remoteReadVersion) {
		assert.True(
			t,
			strings.HasPrefix(remoteReadVersion, "0.1."),
			"unexpected Remote-Read-Version %s, expected 0.1.X",
			remoteReadVersion,
		)
	}

	assert.Equal(t, "custom-header-value", r.Header.Get("Custom-Header-Single"))
	assert.Equal(t, []string{"multiple-1", "multiple-2"}, r.Header.Values("Custom-Header-Multiple"))

	return true
}
