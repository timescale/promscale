package api

import (
	"context"
	"github.com/NYTimes/gziphandler"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"math"
	"net/http"
)

func Labels(queriable *query.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		querier, err := queriable.Querier(context.Background(), math.MinInt64, math.MaxInt64)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}
		names, _, err := querier.LabelNames()
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "internal")
			return
		}

		respond(w, names, nil)
	})

	return gziphandler.GzipHandler(hf)
}
