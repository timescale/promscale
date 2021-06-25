// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/promql"
)

type labelsValue []string

func (l labelsValue) Type() parser.ValueType {
	return parser.ValueTypeNone
}

func (l labelsValue) String() string {
	return strings.Join(l, "\n")
}

func Labels(conf *Config, queryable promql.Queryable) http.Handler {
	hf := corsWrapper(conf, labelsHandler(queryable))
	return gziphandler.GzipHandler(hf)
}

func labelsHandler(queryable promql.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		querier, err := queryable.Samples(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		var names labelsValue
		names, warnings, err := querier.LabelNames()
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		respondLabels(w, &promql.Result{
			Value: names,
		}, warnings)
	}
}

func respondLabels(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setResponseHeaders(w, res, warnings)
	resp := &response{
		Status: "success",
		Data:   res.Value,
	}
	_ = json.NewEncoder(w).Encode(resp)
}
