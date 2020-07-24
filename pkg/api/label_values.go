package api

import (
	"context"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/timescale/timescale-prometheus/pkg/promql"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"math"
	"net/http"
)

func LabelValues(queriable *query.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		name := route.Param(ctx, "name")
		if !model.LabelNameRE.MatchString(name) {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid label name: %s", name), "bad_data")
			return
		}
		querier, err := queriable.Querier(context.Background(), math.MinInt64, math.MaxInt64)
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
	})

	return gziphandler.GzipHandler(hf)
}
