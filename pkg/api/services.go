// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/log"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
)

func Services(conf *Config, reader jaeger_query.JaegerReaderPlugin) http.Handler {
	hf := corsWrapper(conf, servicesHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func servicesHandler(reader jaeger_query.JaegerReaderPlugin) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, err := reader.GetServices(context.Background())
		if err != nil {
			log.Error("msg", fmt.Errorf("get services: %w", err))
			respondError(w, http.StatusInternalServerError, err, "fetching services")
			return
		}
		response := storage_v1.GetServicesResponse{Services: services}
		bSlice, err := response.Marshal()
		if err != nil {
			log.Error("msg", fmt.Errorf("marshal response: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		respondWithByteSlice(w, http.StatusOK, bSlice)
	}
}
