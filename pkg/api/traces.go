// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/log"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
)

func SingleTrace(conf *Config, reader jaeger_query.JaegerReaderPlugin) http.Handler {
	hf := corsWrapper(conf, singleTraceHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func singleTraceHandler(reader jaeger_query.JaegerReaderPlugin) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("into single trace handler")
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", fmt.Errorf("reading request body: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		var traceID storage_v1.GetTraceRequest
		if err = traceID.Unmarshal(b); err != nil {
			log.Error("msg", fmt.Errorf("unmarshalling received trace-id: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("trace id", traceID)
		trace, err := reader.GetTrace(context.Background(), traceID)
		if err != nil {
			log.Error("msg", fmt.Errorf("get-trace: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		bSlice, err := trace.Marshal()
		if err != nil {
			log.Error("msg", fmt.Errorf("marshalling get-trace: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		respondWithByteSlice(w, http.StatusOK, bSlice)
	}
}

func FindTraces(conf *Config, reader jaeger_query.JaegerReaderPlugin) http.Handler {
	hf := corsWrapper(conf, findTracesHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTracesHandler(reader jaeger_query.JaegerReaderPlugin) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", fmt.Errorf("reading request body: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		var findTracesReq storage_v1.FindTracesRequest
		if err = findTracesReq.Unmarshal(b); err != nil {
			log.Error("msg", fmt.Errorf("unmarshalling request: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		traces, err := reader.FindTraces(context.Background(), findTracesReq.Query)
		if err != nil {
			log.Error("msg", fmt.Errorf("find traces: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("traces", traces)
		fmt.Println("traces len", len(traces))

		bSlice, err := json.Marshal(traces)
		if err != nil {
			log.Error("msg", fmt.Errorf("json marshal: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		respondWithByteSliceJSON(w, http.StatusOK, bSlice)
	}
}

func FindTraceIds(conf *Config, reader jaeger_query.JaegerReaderPlugin) http.Handler {
	hf := corsWrapper(conf, findTraceIdsHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTraceIdsHandler(reader jaeger_query.JaegerReaderPlugin) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	}
}
