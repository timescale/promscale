package api

import (
	"context"
	"fmt"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/log"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
	"io/ioutil"
	"net/http"

	"github.com/NYTimes/gziphandler"
)

func SingleTrace(conf *Config, reader *jaeger_query.JaegerQueryReader) http.Handler {
	// todo: remove redundancy from code below
	hf := corsWrapper(conf, singleTraceHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func singleTraceHandler(reader *jaeger_query.JaegerQueryReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("into single trace handler")
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", fmt.Errorf("reading request body: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		var traceID storage_v1.GetTraceRequest
		if err = traceID.Unmarshal(b); err != nil {
			log.Error("msg", fmt.Errorf("unmarshalling received trace-id: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("trace id", traceID)
		trace, err := reader.GetTrace(context.Background(), traceID)
		if err != nil {
			log.Error("msg", fmt.Errorf("get-trace: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		bSlice, err := trace.Marshal()
		if err != nil {
			log.Error("msg", fmt.Errorf("marshalling get-trace: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		respondProto(w, http.StatusOK, bSlice)
	}
}

func FindTraces(conf *Config, reader *jaeger_query.JaegerQueryReader) http.Handler {
	hf := corsWrapper(conf, findTracesHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTracesHandler(reader *jaeger_query.JaegerQueryReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", fmt.Errorf("reading request body: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}

		var findTracesReq storage_v1.FindTracesRequest
		if err = findTracesReq.Unmarshal(b); err != nil {
			log.Error("msg", fmt.Errorf("unmarshalling request: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		traces, err := reader.FindTraces(context.Background(), findTracesReq.Query)
		if err != nil {
			log.Error("msg", fmt.Errorf("find traces: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("api-traces", traces)
		respondProto(w, http.StatusOK, []byte("yo"))
	}
}

func FindTraceIds(conf *Config, reader *jaeger_query.JaegerQueryReader) http.Handler {
	hf := corsWrapper(conf, findTraceIdsHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func findTraceIdsHandler(reader *jaeger_query.JaegerQueryReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		//tLowStr := r.FormValue("trace_id_low")
		//tLow, err := strconv.ParseUint(tLowStr, 10, 64)
		//if err != nil {
		//	respondError(w, http.StatusBadRequest, err, "parsing low trace id uint64")
		//}
		//tHighStr := r.FormValue("trace_id_high")
		//tHigh, err := strconv.ParseUint(tHighStr, 10, 64)
		//if err != nil {
		//	respondError(w, http.StatusBadRequest, err, "parsing high trace id uint64")
		//}
		//traceID := model.TraceID{Low: tLow, High: tHigh}
		//services, err := reader.GetTrace(context.Background(), traceID)
		//if err != nil {
		//	respondError(w, http.StatusInternalServerError, err, "fetching single trace")
		//	return
		//}
		//respond(w, http.StatusOK, services)
	}
}
