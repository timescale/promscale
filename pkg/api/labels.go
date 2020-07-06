package api

import (
	"context"
	"encoding/json"
	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/timescale-prometheus/pkg/promql"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"math"
	"net/http"
	"strings"
)

type labelsValue []string

func (l labelsValue) Type() parser.ValueType {
	return parser.ValueTypeNone
}

func (l labelsValue) String() string {
	return strings.Join(l, "\n")
}

func Labels(queriable *query.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		querier, err := queriable.Querier(context.Background(), math.MinInt64, math.MaxInt64)
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
	})

	return gziphandler.GzipHandler(hf)
}

func respondLabels(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setHeaders(w, res, warnings)
	resp := &response{
		Status: "success",
		Data:   res.Value,
	}
	_ = json.NewEncoder(w).Encode(resp)
}
