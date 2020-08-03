package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/promql"
	"github.com/timescale/timescale-prometheus/pkg/query"
)

func Query(queryEngine *promql.Engine, queryable *query.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ts time.Time
		if t := r.FormValue("time"); t != "" {
			var err error
			ts, err = parseTime(t)
			if err != nil {
				log.Error("msg", "Query error", "err", err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}
		} else {
			ts = time.Now()
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
			return
		}

		respondQuery(w, res, res.Warnings)
	})

	return gziphandler.GzipHandler(hf)
}
