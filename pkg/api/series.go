// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/promql"
)

func Series(conf *Config, queryable promql.Queryable) http.Handler {
	seriesHandler := corsWrapper(conf, series(queryable))
	return gziphandler.GzipHandler(seriesHandler)
}

func series(queryable promql.Queryable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			respondError(w, http.StatusBadRequest, errors.Wrap(err, "error parsing form values"), "bad_data")
			return
		}

		if len(r.Form["match[]"]) == 0 {
			respondError(w, http.StatusBadRequest, errors.New("no match[] parameter provided"), "bad_data")
			return
		}

		start, err := parseTimeParam(r, "start", model.MinTime)
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		end, err := parseTimeParam(r, "end", model.MaxTime)
		if err != nil {
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}
		if end.Before(start) {
			err := errors.New("end timestamp must not be before start time")
			log.Info("msg", "Query bad request:"+err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		var matcherSets [][]*labels.Matcher
		for _, s := range r.Form["match[]"] {
			matchers, err := parser.ParseMetricSelector(s)
			if err != nil {
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}
			matcherSets = append(matcherSets, matchers)
		}
		ctx := r.Context()

		q, err := queryable.Querier(ctx, timestamp.FromTime(start), timestamp.FromTime(end))
		if err != nil {
			respondError(w, http.StatusUnprocessableEntity, err, "execution")
			return
		}
		var sets []storage.SeriesSet
		var warnings storage.Warnings
		for _, mset := range matcherSets {
			s, _ := q.Select(false, nil, nil, nil, mset...)
			warnings = append(warnings, s.Warnings()...)
			if s.Err() != nil {
				respondError(w, http.StatusUnprocessableEntity, s.Err(), "execution")
				return
			}
			sets = append(sets, s)
		}
		set := storage.NewMergeSeriesSet(sets, storage.ChainedSeriesMerge)
		metrics := seriesType{}
		for set.Next() {
			metrics = append(metrics, set.At().Labels())
		}
		if set.Err() != nil {
			respondError(w, http.StatusUnprocessableEntity, set.Err(), "execution")
		}

		sort.Slice(metrics, func(i, j int) bool {
			return labels.Compare(metrics[i], metrics[j]) < 0
		})

		respondSeries(w, &promql.Result{
			Value: metrics,
		}, warnings)
	}
}
func respondSeries(w http.ResponseWriter, res *promql.Result, warnings storage.Warnings) {
	setResponseHeaders(w, res, warnings)
	resp := &response{
		Status: "success",
		Data:   res.Value,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

type seriesType []labels.Labels

func (s seriesType) Type() parser.ValueType {
	return parser.ValueTypeNone
}

func (s seriesType) String() string {
	var str strings.Builder
	for _, l := range s {
		str.WriteString(l.String())
	}
	return str.String()
}
