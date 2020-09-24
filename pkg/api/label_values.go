package api

import (
	"context"
	"fmt"
	"math"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
)

func LabelValues(conf *Config, queryable *query.Queryable) http.Handler {
	hf := corsWrapper(conf, labelValues(queryable))
	return gziphandler.GzipHandler(hf)
}

func labelValues(queryable *query.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		name := route.Param(ctx, "name")
		if !model.LabelNameRE.MatchString(name) {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid label name: %s", name), "bad_data")
			return
		}
		querier, err := queryable.Querier(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		var values labelsValue
		values, warnings, err := querier.LabelValues(name)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}

		respondLabels(w, &promql.Result{
			Value: values,
		}, warnings)
	}
}
