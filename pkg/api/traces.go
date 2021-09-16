package api

import (
	"bytes"
	"fmt"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"net/http"
	"strconv"
	"context"

	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
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

func FindTraces(conf *Config, reader spanstore.Reader) http.Handler {
	hf := corsWrapper(conf, singleTraceHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTracesHandler(reader spanstore.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		_, err := buf.ReadFrom(r.Body)
		if err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("find-traces-handler: %w", err), "request body read error")
			return
		}
		var findTracesReq storage_v1.FindTracesRequest
		if err = findTracesReq.Unmarshal(buf.Bytes()); err != nil {
			respondError(w, http.StatusInternalServerError, fmt.Errorf("", err), "unmarshalling received request")
		}
		respond(w, http.StatusOK, nil)
	}
}

func FindTraceIds(conf *Config, reader spanstore.Reader) http.Handler {
	hf := corsWrapper(conf, singleTraceHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTraceIdsHandler(reader spanstore.Reader) http.HandlerFunc {
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
