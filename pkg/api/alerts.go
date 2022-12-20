// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"net/http"
	"time"

	"github.com/NYTimes/gziphandler"
)

func Alerts(conf *Config, updateMetrics updateMetricCallback) http.Handler {
	hf := corsWrapper(conf, alertsHandler(conf, updateMetrics))
	return gziphandler.GzipHandler(hf)
}

func alertsHandler(apiConf *Config, updateMetrics updateMetricCallback) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		statusCode := "400"
		begin := time.Now()
		defer func() {
			updateMetrics("/api/v1/alerts", statusCode, "", time.Since(begin).Seconds())
		}()

		if apiConf.Rules == nil {
			statusCode = "200"
			respond(w, http.StatusOK, &AlertDiscovery{Alerts: []*Alert{}})
			return
		}

		alerts := []*Alert{}
		alertingRules := apiConf.Rules.AlertingRules()
		for _, alertingRule := range alertingRules {
			alerts = append(
				alerts,
				rulesAlertsToAPIAlerts(alertingRule.ActiveAlerts())...,
			)
		}

		statusCode = "200"
		res := &AlertDiscovery{Alerts: alerts}
		respond(w, http.StatusOK, res)
	}
}
