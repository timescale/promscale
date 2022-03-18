// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
)

func Query(conf *Config, queryEngine *promql.Engine, queryable promql.Queryable, updateMetrics func(handler, code string, duration float64)) http.Handler {
	hf := corsWrapper(conf, queryHandler(queryEngine, queryable, updateMetrics))
	return gziphandler.GzipHandler(hf)
}

func queryHandler(queryEngine *promql.Engine, queryable promql.Queryable, updateMetrics func(handler, code string, duration float64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := "400"
		begin := time.Now()
		defer func() {
			updateMetrics("/api/v1/query", statusCode, time.Since(begin).Seconds())
		}()
		var ts time.Time
		var err error
		ts, err = parseTimeParam(r, "time", time.Now())
		if err != nil {
			log.Error("msg", "Query error", "err", err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			var cancel context.CancelFunc
			timeout, err := parseDuration(to)
			if err != nil {
				log.Error("msg", "Query error", "err", err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		qry, err := queryEngine.NewInstantQuery(queryable, r.FormValue("query"), ts)
		if err != nil {
			log.Error("msg", "Query error", "err", err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		res := qry.Exec(ctx)
		if res.Err != nil {
			log.Error("msg", res.Err, "endpoint", "query")
			var eqcErr promql.ErrQueryCanceled
			if errors.As(res.Err, &eqcErr) {
				statusCode = "503"
				respondError(w, http.StatusServiceUnavailable, res.Err, "canceled")
				return
			}
			var eqtErr promql.ErrQueryTimeout
			if errors.As(res.Err, &eqtErr) {
				statusCode = "503"
				respondError(w, http.StatusServiceUnavailable, res.Err, "timeout")
				return
			}
			var es promql.ErrStorage
			if errors.As(res.Err, &es) {
				statusCode = "500"
				respondError(w, http.StatusInternalServerError, res.Err, "internal")
				return
			}
			statusCode = "422"
			respondError(w, http.StatusUnprocessableEntity, res.Err, "execution")
			return
		}
		statusCode = "2xx"
		respondQuery(w, res, res.Warnings)
	}
}
