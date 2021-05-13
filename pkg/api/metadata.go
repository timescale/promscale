// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"net/http"
	"strconv"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
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
			metric   = r.FormValue("metric")
			limitStr = r.FormValue("limit")
			limit    int64
			err      error
		)
		if limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "converting string to integer")
				return
			}
		}
		data, err := metadata.MetricMetadata(client.Connection, metric, int(limit))
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "fetching metric metadata")
			return
		}
		respond(w, http.StatusOK, data)
	}
}

func TargetMetadata(conf *Config, client *pgclient.Client) http.Handler {
	hf := corsWrapper(conf, targetMetadataHandler(client))
	return gziphandler.GzipHandler(hf)
}

func targetMetadataHandler(client *pgclient.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		var (
			metric      = r.FormValue("metric")
			limitStr    = r.FormValue("limit")
			matchTarget = r.FormValue("match_target")
			limit       int64
			err         error
		)
		if limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "converting string to integer")
				return
			}
		}
		var matchers []*labels.Matcher
		if matchTarget != "" {
			matchers, err = parser.ParseMetricSelector(matchTarget)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}
		} else {
			matcher, err := labels.NewMatcher(labels.MatchRegexp, "job", ".*")
			if err != nil {
				respondError(w, http.StatusInternalServerError, err, "creating all jobs matcher")
				return
			}
			matchers = append(matchers, matcher)
		}
		data, err := metadata.TargetMetadata(client.Connection, matchers, metric, int(limit))
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "fetching metric metadata")
			return
		}
		respond(w, http.StatusOK, data)
	}
}
