package api

import (
	"context"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func QueryRange(conf *Config, queryEngine *promql.Engine, queriable *query.Queryable) http.Handler {
	hf := corsWrapper(conf, queryRange(queryEngine, queriable))
	return gziphandler.GzipHandler(hf)
}

func queryRange(queryEngine *promql.Engine, queriable *query.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, err := parseTime(r.FormValue("start"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		end, err := parseTime(r.FormValue("end"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		if end.Before(start) {
			err := errors.New("end timestamp must not be before start time")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		step, err := parseDuration(r.FormValue("step"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, errors.Wrap(err, "param step"), "bad_data")
			return
		}

		if step <= 0 {
			err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		// For safety, limit the number of returned points per timeseries.
		// This is sufficient for 60s resolution for a week or 1h resolution for a year.
		if end.Sub(start)/step > 11000 {
			err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			var cancel context.CancelFunc
			timeout, err := parseDuration(to)
			if err != nil {
				log.Info("msg", "Query bad request"+err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		qry, err := queryEngine.NewRangeQuery(
			queriable,
			r.FormValue("query"),
			start,
			end,
			step,
		)
		if err != nil {
			log.Info("msg", "Query parse error: "+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		res := qry.Exec(ctx)
		if res.Err != nil {
			log.Error("msg", res.Err, "endpoint", "query_range")
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
