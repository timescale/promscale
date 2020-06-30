package api

import (
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"net/http"
)

func Health(hc pgmodel.HealthChecker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := hc.HealthCheck()
		if err != nil {
			log.Warn("msg", "Healthcheck failed", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	}
}
