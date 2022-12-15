// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/felixge/fgprof"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/timescale/promscale/pkg/api/parser"
	"github.com/timescale/promscale/pkg/ha"
	haClient "github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/jaeger"
	jaegerStore "github.com/timescale/promscale/pkg/jaeger/store"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	pgMetrics "github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/psctx"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/telemetry"
)

// TODO: Refactor this function to reduce number of paramaters.
func GenerateRouter(apiConf *Config, promqlConf *query.Config, client *pgclient.Client, store *jaegerStore.Store, authWrapper mux.MiddlewareFunc, reload func() error) (*mux.Router, error) {
	var writePreprocessors []parser.Preprocessor
	if apiConf.HighAvailability {
		service := ha.NewService(haClient.NewLeaseClient(client.ReadOnlyConnection()))
		writePreprocessors = append(writePreprocessors, ha.NewFilter(service))
	}
	if apiConf.MultiTenancy != nil {
		writePreprocessors = append(writePreprocessors, apiConf.MultiTenancy.WriteAuthorizer())
	}

	dataParser := parser.NewParser()
	for _, preproc := range writePreprocessors {
		dataParser.AddPreprocessor(preproc)
	}

	writeHandler := timeHandler(metrics.HTTPRequestDuration, "write", otelhttp.NewHandler(Write(client, dataParser, updateIngestMetrics), "write-metrics"))

	// If we are running in read-only mode, log and send NotFound status.
	if apiConf.ReadOnly {
		writeHandler = withWarnLog("trying to send metrics to write API while connector is in read-only mode", http.NotFoundHandler())
	}

	router := mux.NewRouter().UseEncodedPath()
	if authWrapper != nil {
		router.Use(authWrapper)
	}

	router.Path("/write").Methods(http.MethodPost).HandlerFunc(writeHandler)

	readHandler := timeHandler(metrics.HTTPRequestDuration, "read", Read(apiConf, client, metrics, updateQueryMetrics))
	router.Path("/read").Methods(http.MethodGet, http.MethodPost).HandlerFunc(readHandler)

	deleteHandler := timeHandler(metrics.HTTPRequestDuration, "delete_series", Delete(apiConf, client))
	router.Path("/delete_series").Methods(http.MethodPut, http.MethodPost).HandlerFunc(deleteHandler)

	queryable := client.Queryable()
	queryEngine := client.QueryEngine()

	apiV1 := router.PathPrefix("/api/v1").Subrouter()
	queryHandler := timeHandler(metrics.HTTPRequestDuration, "query", Query(apiConf, queryEngine, queryable, updateQueryMetrics))
	apiV1.Path("/query").Methods(http.MethodGet, http.MethodPost).HandlerFunc(queryHandler)

	queryRangeHandler := timeHandler(metrics.HTTPRequestDuration, "query_range", QueryRange(apiConf, promqlConf, queryEngine, queryable, updateQueryMetrics))
	apiV1.Path("/query_range").Methods(http.MethodGet, http.MethodPost).HandlerFunc(queryRangeHandler)

	exemplarQueryHandler := timeHandler(metrics.HTTPRequestDuration, "query_exemplar", QueryExemplar(apiConf, queryable, updateQueryMetrics))
	apiV1.Path("/query_exemplars").Methods(http.MethodGet, http.MethodPost).HandlerFunc(exemplarQueryHandler)

	seriesHandler := timeHandler(metrics.HTTPRequestDuration, "series", Series(apiConf, queryable))
	apiV1.Path("/series").Methods(http.MethodGet, http.MethodPost).HandlerFunc(seriesHandler)

	labelsHandler := timeHandler(metrics.HTTPRequestDuration, "labels", Labels(apiConf, queryable))
	apiV1.Path("/labels").Methods(http.MethodGet, http.MethodPost).HandlerFunc(labelsHandler)

	metadataHandler := timeHandler(metrics.HTTPRequestDuration, "metadata", MetricMetadata(apiConf, client))
	apiV1.Path("/metadata").Methods(http.MethodGet, http.MethodPost).HandlerFunc(metadataHandler)

	rulesHandler := timeHandler(metrics.HTTPRequestDuration, "rules", Rules(apiConf, updateQueryMetrics))
	apiV1.Path("/rules").Methods(http.MethodGet).HandlerFunc(rulesHandler)

	alertsHandler := timeHandler(metrics.HTTPRequestDuration, "alerts", Alerts(apiConf, updateQueryMetrics))
	apiV1.Path("/alerts").Methods(http.MethodGet).HandlerFunc(alertsHandler)

	labelValuesHandler := timeHandler(metrics.HTTPRequestDuration, "label/:name/values", LabelValues(apiConf, queryable))
	apiV1.Path("/label/{name}/values").Methods(http.MethodGet).HandlerFunc(labelValuesHandler)

	healthChecker := func() error { return client.HealthCheck() }
	router.Path("/healthz").Methods(http.MethodGet, http.MethodOptions, http.MethodHead).HandlerFunc(Health(healthChecker))
	router.Path(apiConf.TelemetryPath).Methods(http.MethodGet).HandlerFunc(promhttp.Handler().ServeHTTP)

	reloadHandler := timeHandler(metrics.HTTPRequestDuration, "/-/reload", Reload(reload, apiConf.AdminAPIEnabled))
	router.Path("/-/reload").Methods(http.MethodPost).HandlerFunc(reloadHandler)

	if store != nil {
		jaeger.ExtendQueryAPIs(router, client.ReadOnlyConnection(), store)
	}

	debugProf := router.PathPrefix("/debug/pprof").Subrouter()
	debugProf.Path("").Methods(http.MethodGet).HandlerFunc(pprof.Index)
	debugProf.Path("/cmdline").Methods(http.MethodGet).HandlerFunc(pprof.Cmdline)
	debugProf.Path("/profile").Methods(http.MethodGet).HandlerFunc(pprof.Profile)
	debugProf.Path("/symbol").Methods(http.MethodGet).HandlerFunc(pprof.Symbol)
	debugProf.Path("/trace").Methods(http.MethodGet).HandlerFunc(pprof.Trace)
	debugProf.Path("/heap").Methods(http.MethodGet).HandlerFunc(pprof.Handler("heap").ServeHTTP)
	debugProf.Path("/goroutine").Methods(http.MethodGet).HandlerFunc(pprof.Handler("goroutine").ServeHTTP)
	debugProf.Path("/threadcreate").Methods(http.MethodGet).HandlerFunc(pprof.Handler("threadcreate").ServeHTTP)
	debugProf.Path("/block").Methods(http.MethodGet).HandlerFunc(pprof.Handler("block").ServeHTTP)
	debugProf.Path("/allocs").Methods(http.MethodGet).HandlerFunc(pprof.Handler("allocs").ServeHTTP)
	debugProf.Path("/mutex").Methods(http.MethodGet).HandlerFunc(pprof.Handler("mutex").ServeHTTP)

	router.Path("/debug/fgprof").Methods(http.MethodGet).HandlerFunc(fgprof.Handler().ServeHTTP)
	return router, nil
}

func RegisterTelemetryMetrics(t telemetry.Engine) error {
	var err error
	if err = t.RegisterMetric(
		"promscale_ingested_samples_total",
		pgMetrics.IngestorItems.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"})); err != nil {
		return fmt.Errorf("register 'promscale_ingested_samples_total' metric for telemetry: %w", err)
	}
	if err = t.RegisterMetric(
		"promscale_metrics_queries_failed_total",
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query", "code": "422"}),
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query_range", "code": "422"}),
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query", "code": "500"}),
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query_range", "code": "500"}),
	); err != nil {
		return fmt.Errorf("register 'promscale_metrics_queries_failed_total' metric for telemetry: %w", err)
	}
	if err = t.RegisterMetric(
		"promscale_metrics_queries_success_total",
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query", "code": "2xx"}),
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query_range", "code": "2xx"}),
	); err != nil {
		return fmt.Errorf("register 'promscale_metrics_queries_success_total' metric for telemetry: %w", err)
	}
	if err = t.RegisterMetric(
		"promscale_metrics_queries_timedout_total",
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query", "code": "503"}),
		pgMetrics.Query.With(prometheus.Labels{"type": "metric", "handler": "/api/v1/query_range", "code": "503"}),
	); err != nil {
		return fmt.Errorf("register 'promscale_metrics_queries_timedout_total' metric for telemetry: %w", err)
	}
	return nil
}

func withWarnLog(msg string, handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Warn("msg", msg)
		handler.ServeHTTP(w, r)
	}
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(histogramVec prometheus.ObserverVec, path string, handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := psctx.WithStartTime(r.Context(), start)
		r = r.WithContext(ctx)
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Milliseconds()
		histogramVec.WithLabelValues(path).Observe(float64(elapsedMs))
	}
}
