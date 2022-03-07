// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"fmt"
	"math"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/gorilla/mux"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/promql"
)

func LabelValues(conf *Config, queryable promql.Queryable) http.Handler {
	hf := corsWrapper(conf, labelValues(queryable))
	return gziphandler.GzipHandler(hf)
}

func labelValues(queryable promql.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := mux.Vars(r)["name"]
		if !model.LabelNameRE.MatchString(name) {
			respondError(w, http.StatusBadRequest, fmt.Errorf("invalid label name: %s", name), "bad_data")
			return
		}
		querier, err := queryable.SamplesQuerier(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		defer querier.Close()

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
