// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
)

func Query(conf *Config, queryEngine *promql.Engine, queryable promql.Queryable, metrics *Metrics) http.Handler {
	hf := corsWrapper(conf, queryHandler(queryEngine, queryable, metrics))
	return gziphandler.GzipHandler(hf)
}

func queryHandler(queryEngine *promql.Engine, queryable promql.Queryable, metrics *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var ts time.Time
		var err error
		ts, err = parseTimeParam(r, "time", time.Now())
		if err != nil {
			log.Error("msg", "Query error", "err", err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			var cancel context.CancelFunc
			timeout, err := ParseDuration(to)
			if err != nil {
				log.Error("msg", "Query error", "err", err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				metrics.InvalidQueryReqs.Add(1)
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		metrics.ReceivedQueries.Add(1)
		begin := time.Now()
		qry, err := queryEngine.NewInstantQuery(queryable, r.FormValue("query"), ts)
		if err != nil {
			log.Error("msg", "Query error", "err", err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.FailedQueries.Add(1)
			return
		}

		res := qry.Exec(ctx)
		metrics.QueryDuration.Observe(time.Since(begin).Seconds())

		if res.Err != nil {
			log.Error("msg", res.Err, "endpoint", "query")
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				respondError(w, http.StatusServiceUnavailable, res.Err, "canceled")
				return
			case promql.ErrQueryTimeout:
				respondError(w, http.StatusServiceUnavailable, res.Err, "timeout")
				return
			case promql.ErrStorage:
				respondError(w, http.StatusInternalServerError, res.Err, "internal")
				return
			}
			respondError(w, http.StatusUnprocessableEntity, res.Err, "execution")
			metrics.FailedQueries.Add(1)
			return
		}

		respondQuery(w, res, res.Warnings)
	}
}
