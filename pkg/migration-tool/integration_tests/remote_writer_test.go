package integration_tests

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/util/testutil"
)

type remoteWriteServer struct {
	mux                    sync.RWMutex
	writeServer            *httptest.Server
	writeStorageTimeSeries map[string]prompb.TimeSeries
}

// createRemoteWriteServer creates a write server that exposes a /write endpoint for ingesting samples. It returns a server
// which is expected to be closed by the caller.
func createRemoteWriteServer(t *testing.T) (*remoteWriteServer, string) {
	rws := &remoteWriteServer{
		writeStorageTimeSeries: make(map[string]prompb.TimeSeries),
	}
	s := httptest.NewServer(getWriteHandler(t, rws))
	rws.writeServer = s
	return rws, s.URL
}

// Series returns the number of series in the remoteWriteServer.
func (rwss *remoteWriteServer) Series() int {
	rwss.mux.RLock()
	defer rwss.mux.RUnlock()
	return len(rwss.writeStorageTimeSeries)
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

// Close closes the server(s).
func (rwss *remoteWriteServer) Close() {
	rwss.writeServer.Close()
}

func getWriteHandler(t *testing.T, rws *remoteWriteServer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		// we treat invalid requests as the same as no request for
		// leadership-timeout purposes
		if !validateWriteHeaders(t, w, r) {
			t.Fatal("could not validate write headers")
		}

		reqBuf, err, msg := decodeSnappyBody(r.Body)
		if err != nil {
			t.Fatal("msg", msg, "err", err.Error())
		}
		req := NewWriteRequest()
		err = proto.Unmarshal(reqBuf, req)
		testutil.Ok(t, err)

		rws.mux.Lock()
		defer rws.mux.Unlock()
		if len(req.Timeseries) > 0 {
			for _, ts := range req.Timeseries {
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
	compressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err, "Read error"
	}

	buf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err, "Snappy decode error"
	}

	return buf, nil, ""
}

func validateWriteHeaders(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	if r.Method != "POST" {
		t.Fatalf("HTTP Method %s instead of POST", r.Method)
	}

	switch r.Header.Get("Content-Type") {
	case "application/x-protobuf":
		if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
			t.Fatalf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding"))
		}

		remoteWriteVersion := r.Header.Get("X-Prometheus-Remote-Write-Version")
		if remoteWriteVersion == "" {
			t.Fatal("msg", "Missing X-Prometheus-Remote-Write-Version header")
		}

		if !strings.HasPrefix(remoteWriteVersion, "0.1.") {
			t.Fatalf("unexpected Remote-Write-Version %s, expected 0.1.X", remoteWriteVersion)
		}
	case "application/json":
		// Don't need any other header checks for JSON content type.
	default:
		t.Fatal("unsupported data format (not protobuf or json)")
	}

	return true
}
