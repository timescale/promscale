package api

import (
	"context"
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/promql"
)

func QueryExemplar(conf *Config, queryable promql.Queryable, metrics *Metrics) http.Handler {
	hf := corsWrapper(conf, queryExemplar(queryable, metrics))
	return gziphandler.GzipHandler(hf)
}

func queryExemplar(queryable promql.Queryable, metrics *Metrics) http.HandlerFunc {
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

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			// Note: Prometheus does not implement timeout for querying exemplars.
			// But I think we should keep this as optional.
			var cancel context.CancelFunc
			timeout, err := parseDuration(to)
			if err != nil {
				log.Info("msg", "Query bad request"+err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				metrics.InvalidQueryReqs.Inc()
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		begin := time.Now()
		expr, err := parser.ParseExpr(r.FormValue("query"))
		if err != nil {
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		selectors := parser.ExtractSelectors(expr)
		querier := queryable.Exemplar(ctx)
		results, err := querier.Select(start, end, selectors...)
		if err != nil {
			log.Error("msg", err, "endpoint", "query_range")
			respondError(w, http.StatusInternalServerError, err, "execution_error")
			return
		}
		metrics.QueryDuration.Observe(time.Since(begin).Seconds())
		respondExemplar(w, results)
	}
}
