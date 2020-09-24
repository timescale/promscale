package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func Query(conf *Config, queryEngine *promql.Engine, queryable *query.Queryable) http.Handler {
	hf := corsWrapper(conf, queryHandler(queryEngine, queryable))
	return gziphandler.GzipHandler(hf)
}

func queryHandler(queryEngine *promql.Engine, queryable *query.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}
