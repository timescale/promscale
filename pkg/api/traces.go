package api

import (
	"context"
	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"net/http"
	"strconv"
)

func SingleTrace(conf *Config, reader spanstore.Reader) http.Handler {
	// todo: remove redundancy from code below
	hf := corsWrapper(conf, singleTraceHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func singleTraceHandler(reader spanstore.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tLowStr := r.FormValue("trace_id_low")
		tLow, err := strconv.ParseUint(tLowStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err, "parsing low trace id uint64")
		}
		tHighStr := r.FormValue("trace_id_high")
		tHigh, err := strconv.ParseUint(tHighStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, err, "parsing high trace id uint64")
		}
		traceID := model.TraceID{Low: tLow, High: tHigh}
		services, err := reader.GetTrace(context.Background(), traceID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, err, "fetching single trace")
			return
		}
		respond(w, http.StatusOK, services)
	}
}
