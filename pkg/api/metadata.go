// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"net/http"
	"strconv"

	"github.com/NYTimes/gziphandler"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/metadata"
)

func MetricMetadata(conf *Config, client *pgclient.Client) http.Handler {
	hf := corsWrapper(conf, metricMetadataHandler(client))
	return gziphandler.GzipHandler(hf)
}

func metricMetadataHandler(client *pgclient.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		var (
			limit int64
			err   error

			metric   = r.FormValue("metric")
			limitStr = r.FormValue("limit")
		)
		if limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "converting string to integer")
				return
			}
		}
		data, err := metadata.MetricQuery(client.Connection, metric, int(limit))
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "fetching metric metadata")
			return
		}
		respond(w, http.StatusOK, data)
	}
}
