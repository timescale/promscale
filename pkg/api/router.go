package api

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/route"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/util"
)

func GenerateRouter(apiConf *Config, metrics *Metrics, client *pgclient.Client, elector *util.Elector) http.Handler {
	router := route.New()
	writeHandler := timeHandler(metrics.HTTPRequestDuration, "write", Write(client, elector, metrics))
	router.Post("/write", writeHandler)

	readHandler := timeHandler(metrics.HTTPRequestDuration, "read", Read(client, metrics))
	router.Get("/read", readHandler)
	router.Post("/read", readHandler)

	queryable := client.GetQueryable()
	queryEngine := query.NewEngine(log.GetLogger(), time.Minute)
	queryHandler := timeHandler(metrics.HTTPRequestDuration, "query", Query(apiConf, queryEngine, queryable))
	router.Get("/api/v1/query", queryHandler)
	router.Post("/api/v1/query", queryHandler)

	queryRangeHandler := timeHandler(metrics.HTTPRequestDuration, "query_range", QueryRange(apiConf, queryEngine, queryable))
	router.Get("/api/v1/query_range", queryRangeHandler)
	router.Post("/api/v1/query_range", queryRangeHandler)

	seriesHandler := timeHandler(metrics.HTTPRequestDuration, "series", Series(apiConf, queryable))
	router.Get("/api/v1/series", seriesHandler)
	router.Post("/api/v1/series", seriesHandler)

	labelsHandler := timeHandler(metrics.HTTPRequestDuration, "labels", Labels(apiConf, queryable))
	router.Get("/api/v1/labels", labelsHandler)
	router.Post("/api/v1/labels", labelsHandler)

	labelValuesHandler := timeHandler(metrics.HTTPRequestDuration, "label/:name/values", LabelValues(apiConf, queryable))
	router.Get("/api/v1/label/:name/values", labelValuesHandler)

	router.Get("/healthz", Health(client))

	return router
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(histogramVec prometheus.ObserverVec, path string, handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Milliseconds()
		histogramVec.WithLabelValues(path).Observe(float64(elapsedMs))
	}
}
