package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/prompb"
)

func Read(reader pgmodel.Reader, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validateReadHeaders(w, r) {
			metrics.InvalidReadReqs.Inc()
			return
		}

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read header validation error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		queryCount := float64(len(req.Queries))
		metrics.ReceivedQueries.Add(queryCount)
		begin := time.Now()

		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.Warn("msg", "Error executing query", "query", req, "storage", "PostgreSQL", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			metrics.FailedQueries.Add(queryCount)
			return
		}

		duration := time.Since(begin).Seconds()
		metrics.QueryBatchDuration.Observe(duration)

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			metrics.FailedQueries.Add(queryCount)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			metrics.FailedQueries.Add(queryCount)
			return
		}
	})
}

func validateReadHeaders(w http.ResponseWriter, r *http.Request) bool {
	// validate headers from https://github.com/prometheus/prometheus/blob/2bd077ed9724548b6a631b6ddba48928704b5c34/storage/remote/client.go
	if r.Method != "POST" {
		buildReadError(w, fmt.Sprintf("HTTP Method %s instead of POST", r.Method))
		return false
	}

	if !strings.Contains(r.Header.Get("Content-Encoding"), "snappy") {
		buildReadError(w, fmt.Sprintf("non-snappy compressed data got: %s", r.Header.Get("Content-Encoding")))
		return false
	}

	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		buildReadError(w, "non-protobuf data")
		return false
	}

	remoteReadVersion := r.Header.Get("X-Prometheus-Remote-Read-Version")
	if remoteReadVersion == "" {
		err := "missing X-Prometheus-Remote-Read-Version"
		log.Warn("msg", "Read header validation error", "err", err)
	} else if !strings.HasPrefix(remoteReadVersion, "0.1.") {
		buildReadError(w, fmt.Sprintf("unexpected Remote-Read-Version %s, expected 0.1.X", remoteReadVersion))
		return false
	}

	return true
}

func buildReadError(w http.ResponseWriter, err string) {
	log.Error("msg", "Read header validation error", "err", err)
	http.Error(w, err, http.StatusBadRequest)
}
