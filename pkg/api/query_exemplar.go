// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/exemplar"
	"github.com/timescale/promscale/pkg/promql"
)

func QueryExemplar(conf *Config, queryable promql.Queryable, updateMetrics updateMetricCallback) http.Handler {
	hf := corsWrapper(conf, queryExemplar(queryable, updateMetrics))
	return gziphandler.GzipHandler(hf)
}

func queryExemplar(queryable promql.Queryable, updateMetrics updateMetricCallback) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := "400"
		begin := time.Now()
		defer func() {
			updateMetrics("/api/v1/query_exemplars", statusCode, "", time.Since(begin).Seconds())
		}()
		start, err := parseTime(r.FormValue("start"))
		if err != nil {
			log.Info("msg", "Exemplar query bad request:", "error", err)
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		end, err := parseTime(r.FormValue("end"))
		if err != nil {
			log.Info("msg", "Exemplar query bad request:", "error", err)
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		if end.Before(start) {
			err := errors.New("end timestamp must not be before start time")
			log.Info("msg", "Exemplar query bad request:", "error", err)
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		ctx := r.Context()
		if timeout := r.FormValue("timeout"); timeout != "" {
			// Note: Prometheus does not implement timeout for querying exemplars.
			// But we should keep this as optional.
			var cancel context.CancelFunc
			timeout, err := parseDuration(timeout)
			if err != nil {
				log.Info("msg", "Exemplar query bad request:", "error", err)
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		results, err := exemplar.QueryExemplar(ctx, r.FormValue("query"), queryable, start, end)
		if err != nil {
			statusCode = "500"
			log.Error("msg", err, "endpoint", "query_exemplars")
			respondError(w, http.StatusInternalServerError, err, "bad_data")
			return
		}
		statusCode = "2xx"
		respondExemplar(w, results)
	}
}
