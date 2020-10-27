package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel"
)

func Delete(conf *Config, client *pgclient.Client) http.Handler {
	hf := corsWrapper(conf, deleteHandler(client))
	return gziphandler.GzipHandler(hf)
}

func deleteHandler(client *pgclient.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			metricsTouched     []string
			totalChunksTouched int
			seriesDeleted      []pgmodel.SeriesID
		)
		if err := r.ParseForm(); err != nil {
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		for _, s := range r.Form["match[]"] {
			matchers, err := parser.ParseMetricSelector(s)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}
			pgDeleter := client.Deleter().PgDeleter()
			touchedMetrics, deletedSeriesIDs, chunksTouched, err := pgDeleter.DeleteSeries(matchers)
			if err != nil {
				respondErrorWithMessage(w, http.StatusInternalServerError, err, "deleting_series",
					fmt.Sprintf("partial delete: deleted %s series IDs from %s metrics, touching %d chunks in total",
						strings.Join(distinctValues(seriesDeleted), ", "),
						strings.Join(distinctValues(metricsTouched), ", "),
						totalChunksTouched,
					),
				)
				return
			}
			metricsTouched = append(metricsTouched, touchedMetrics...)
			seriesDeleted = append(seriesDeleted, deletedSeriesIDs...)
			totalChunksTouched += chunksTouched
		}
		respond(w, http.StatusOK,
			fmt.Sprintf("deleted %s series IDs from %s metrics, touching %d chunks in total",
				strings.Join(distinctValues(seriesDeleted), ", "),
				strings.Join(distinctValues(metricsTouched), ", "),
				totalChunksTouched,
			),
		)
	}
}

func distinctValues(slice interface{}) []string {
	var (
		temp = make(map[string]struct{})
		keys []string
	)
	switch elem := slice.(type) {
	case []string:
		for _, element := range elem {
			if _, ok := temp[element]; !ok {
				temp[element] = struct{}{}
			}
		}
	case []pgmodel.SeriesID:
		for _, element := range elem {
			stringElement := strconv.FormatInt(int64(element), 10)
			if _, ok := temp[stringElement]; !ok {
				temp[stringElement] = struct{}{}
			}
		}
	}
	for k := range temp {
		keys = append(keys, k)
	}
	return keys
}
