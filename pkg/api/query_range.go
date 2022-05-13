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
	"github.com/timescale/promscale/pkg/query"
)

func QueryRange(conf *Config, promqlConf *query.Config, queryEngine *promql.Engine, queryable promql.Queryable, updateMetrics func(handler, code string, duration float64)) http.Handler {
	hf := corsWrapper(conf, queryRange(promqlConf, queryEngine, queryable, updateMetrics))
	return gziphandler.GzipHandler(hf)
}

func queryRange(promqlConf *query.Config, queryEngine *promql.Engine, queryable promql.Queryable, updateMetrics func(handler, code string, duration float64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := "400"
		begin := time.Now()
		defer func() {
			updateMetrics("/api/v1/query_range", statusCode, time.Since(begin).Seconds())
		}()
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
		if int64(end.Sub(start)/step) > promqlConf.MaxPointsPerTs {
			err := fmt.Errorf("exceeded maximum resolution of %d points per timeseries. Try decreasing the query resolution (?step=XX) or "+
				"increasing the 'promql-max-points-per-ts' limit", promqlConf.MaxPointsPerTs)
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
			queryable,
			nil,
			r.FormValue("query"),
			start,
			end,
			step,
		)
		if err != nil {
			statusCode = "400"
			log.Info("msg", "Query parse error: "+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		res := qry.Exec(ctx)

		if res.Err != nil {
			log.Error("msg", res.Err, "endpoint", "query_range")
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				statusCode = "503"
				respondError(w, http.StatusServiceUnavailable, res.Err, "canceled")
				return
			case promql.ErrQueryTimeout:
				statusCode = "503"
				respondError(w, http.StatusServiceUnavailable, res.Err, "timeout")
				return
			case promql.ErrStorage:
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
