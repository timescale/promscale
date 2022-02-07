// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/log"
	pgMetrics "github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/prompb"
)

func Read(config *Config, reader querier.Reader, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validateReadHeaders(w, r) {
			metrics.InvalidReadReqs.Inc()
			return
		}

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read header validation error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "400"}).Inc()
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "400"}).Inc()
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "400"}).Inc()
			return
		}

		pgMetrics.RemoteReadReceivedQueries.Add(float64(len(req.Queries)))
		begin := time.Now()

		// Drop __replica__ labelSet when
		// Promscale is running is HA mode
		// as the same lebelSet is dropped during ingestion.
		if config.HighAvailability {
			for _, q := range req.Queries {
				for ind, l := range q.Matchers {
					if l.Name == ha.ReplicaNameLabel {
						q.Matchers = append(q.Matchers[:ind], q.Matchers[ind+1:]...)
					}
				}
			}
		}

		var resp *prompb.ReadResponse
		resp, err = reader.Read(&req)
		if err != nil {
			log.Warn("msg", "Error executing query", "query", req, "storage", "PostgreSQL", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "500"}).Inc()
			return
		}

		duration := time.Since(begin).Seconds()
		pgMetrics.QueryDuration.With(prometheus.Labels{"type": "metric", "handler": "/read"}).Observe(duration)

		data, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "500"}).Inc()
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/read", "code": "500"}).Inc()
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
