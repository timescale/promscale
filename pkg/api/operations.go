package api

import (
	"context"
	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"net/http"
)

func Operations(conf *Config, reader spanstore.Reader) http.Handler {
	hf := corsWrapper(conf, operationsHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func operationsHandler(reader spanstore.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		services, err := reader.GetServices(context.Background())
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "fetching services")
			return
		}
		respond(w, http.StatusOK, services)
	}
}
