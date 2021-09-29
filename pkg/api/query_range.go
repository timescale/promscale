// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
)

func QueryRange(conf *Config, queryEngine *promql.Engine, queryable promql.Queryable, metrics *Metrics) http.Handler {
	hf := corsWrapper(conf, queryRange(conf, queryEngine, queryable, metrics))
	return gziphandler.GzipHandler(hf)
}

func queryRange(conf *Config, queryEngine *promql.Engine, queryable promql.Queryable, metrics *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, err := parseTime(r.FormValue("start"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}
		end, err := parseTime(r.FormValue("end"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}
		if end.Before(start) {
			err := errors.New("end timestamp must not be before start time")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}

		step, err := ParseDuration(r.FormValue("step"))
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, errors.Wrap(err, "param step"), "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}

		if step <= 0 {
			err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}

		// For safety, limit the number of returned points per timeseries.
		// This is sufficient for 60s resolution for a week or 1h resolution for a year.
		if int64(end.Sub(start)/step) > conf.MaxPointsPerTs {
			err := fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX) or "+
				"increasing the 'promql-max-points-per-ts' limit", conf.MaxPointsPerTs)
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.InvalidQueryReqs.Add(1)
			return
		}

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			var cancel context.CancelFunc
			timeout, err := ParseDuration(to)
			if err != nil {
				log.Info("msg", "Query bad request"+err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				metrics.InvalidQueryReqs.Add(1)
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		metrics.ReceivedQueries.Add(1)
		begin := time.Now()
		qry, err := queryEngine.NewRangeQuery(
			queryable,
			r.FormValue("query"),
			start,
			end,
			step,
		)
		if err != nil {
			log.Info("msg", "Query parse error: "+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			metrics.FailedQueries.Add(1)
			return
		}

		res := qry.Exec(ctx)
		metrics.QueryDuration.Observe(time.Since(begin).Seconds())

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
			metrics.FailedQueries.Add(1)
			return
		}

		respondQuery(w, res, res.Warnings)
	}
}
