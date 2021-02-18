// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/route"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/util"
)

func GenerateRouter(apiConf *Config, metrics *Metrics, client *pgclient.Client, elector *util.Elector) (http.Handler, error) {
	authWrapper := func(name string, h http.HandlerFunc) http.HandlerFunc {
		return authHandler(apiConf, h)
	}

	router := route.New().WithInstrumentation(authWrapper)

	writeHandler := timeHandler(metrics.HTTPRequestDuration, "write", Write(client, elector, metrics))

	// If we are running in read-only mode, log and send NotFound status.
	if apiConf.ReadOnly {
		writeHandler = withWarnLog("trying to send metrics to write API while connector is in read-only mode", http.NotFoundHandler())
	}

	router.Post("/write", writeHandler)

	readHandler := timeHandler(metrics.HTTPRequestDuration, "read", Read(client, metrics))
	router.Get("/read", readHandler)
	router.Post("/read", readHandler)

	deleteHandler := timeHandler(metrics.HTTPRequestDuration, "delete_series", Delete(apiConf, client))
	router.Put("/delete_series", deleteHandler)
	router.Post("/delete_series", deleteHandler)

	queryable := client.Queryable()
	queryEngine, err := query.NewEngine(log.GetLogger(), apiConf.MaxQueryTimeout, apiConf.SubQueryStepInterval, apiConf.EnabledFeaturesList)
	if err != nil {
		return nil, fmt.Errorf("creating query-engine: %w", err)
	}
	queryHandler := timeHandler(metrics.HTTPRequestDuration, "query", Query(apiConf, queryEngine, queryable, metrics))
	router.Get("/api/v1/query", queryHandler)
	router.Post("/api/v1/query", queryHandler)

	queryRangeHandler := timeHandler(metrics.HTTPRequestDuration, "query_range", QueryRange(apiConf, queryEngine, queryable, metrics))
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

	healthChecker := func() error { return client.HealthCheck() }
	router.Get("/healthz", Health(healthChecker))

	router.Get(apiConf.TelemetryPath, promhttp.Handler().ServeHTTP)
	router.Get("/debug/pprof/", pprof.Index)
	router.Get("/debug/pprof/cmdline", pprof.Cmdline)
	router.Get("/debug/pprof/profile", pprof.Profile)
	router.Get("/debug/pprof/symbol", pprof.Symbol)
	router.Get("/debug/pprof/trace", pprof.Trace)
	router.Get("/debug/pprof/heap", pprof.Handler("heap").ServeHTTP)

	return router, nil
}

func authHandler(cfg *Config, handler http.HandlerFunc) http.HandlerFunc {
	if cfg.Auth == nil {
		return handler
	}

	if cfg.Auth.BasicAuthUsername != "" {
		return func(w http.ResponseWriter, r *http.Request) {
			user, pass, ok := r.BasicAuth()
			if !ok || cfg.Auth.BasicAuthUsername != user || cfg.Auth.BasicAuthPassword != pass {
				log.Error("msg", "Unauthorized access to endpoint, invalid username or password")
				http.Error(w, "Unauthorized access to endpoint, invalid username or password.", http.StatusUnauthorized)
				return
			}
			handler.ServeHTTP(w, r)
		}
	}

	if cfg.Auth.BearerToken != "" {
		return func(w http.ResponseWriter, r *http.Request) {
			splitToken := strings.Split(r.Header.Get("Authorization"), "Bearer ")
			if cfg.Auth.BearerToken != splitToken[1] {
				log.Error("msg", "Unauthorized access to endpoint, invalid bearer token")
				http.Error(w, "Unauthorized access to endpoint, invalid bearer token.", http.StatusUnauthorized)
				return
			}
			handler.ServeHTTP(w, r)
		}
	}

	return handler
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
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Milliseconds()
		histogramVec.WithLabelValues(path).Observe(float64(elapsedMs))
	}
}
