package api

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/timescale/timescale-prometheus/pkg/log"
	tspromql "github.com/timescale/timescale-prometheus/pkg/promql"
)

func Query(queryEngine *promql.Engine, queriable *tspromql.Queryable) http.Handler {
	hf := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ts time.Time
		if t := r.FormValue("time"); t != "" {
			var err error
			ts, err = parseTime(t)
			if err != nil {
				log.Error("msg", "Query error", "err", err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}
		} else {
			ts = time.Now()
		}

		ctx := r.Context()
		if to := r.FormValue("timeout"); to != "" {
			var cancel context.CancelFunc
			timeout, err := parseDuration(to)
			if err != nil {
				log.Error("msg", "Query error", "err", err.Error())
				respondError(w, http.StatusBadRequest, err, "bad_data")
				return
			}

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		qry, err := queryEngine.NewInstantQuery(queriable, r.FormValue("query"), ts)
		if err != nil {
			log.Error("msg", "Query error", "err", err.Error())
			respondError(w, http.StatusBadRequest, err, "bad_data")
			return
		}

		res := qry.Exec(ctx)
		if res.Err != nil {
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				respondError(w, http.StatusServiceUnavailable, res.Err, "canceled")
				return
			case promql.ErrQueryTimeout:
				respondError(w, http.StatusServiceUnavailable, res.Err, "timeout")
				return
			case promql.ErrStorage:
				respondError(w, http.StatusInternalServerError, res.Err, "internal")
				return
			}
			respondError(w, http.StatusUnprocessableEntity, res.Err, "execution")
			return
		}

		if res != nil {
			respond(w, res)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	return gziphandler.GzipHandler(hf)
}

func respond(w http.ResponseWriter, res *promql.Result) {
	w.Header().Set("Content-Type", "application/json")
	if len(res.Warnings) > 0 {
		w.Header().Set("Cache-Control", "no-store")
	}
	w.WriteHeader(http.StatusOK)

	resp := &response{
		Status: "success",
		Data:   res.Value,
	}
	for _, warn := range res.Warnings {
		resp.Warnings = append(resp.Warnings, warn.Error())
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func respondError(w http.ResponseWriter, status int, err error, errType string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&errResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     err.Error(),
	})
}

type errResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

type response struct {
	Status   string      `json:"status"`
	Data     interface{} `json:"data,omitempty"`
	Warnings []string    `json:"warnings,omitempty"`
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
}
