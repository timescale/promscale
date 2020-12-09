package api

import (
	"net/http"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
)

func Health(hc pgmodel.HealthCheckerFn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := hc()
		if err != nil {
			log.Warn("msg", "Healthcheck failed", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	}
}
