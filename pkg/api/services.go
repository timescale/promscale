package api

import (
	"context"
	"fmt"
	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/log"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
	"net/http"
)

func Services(conf *Config, reader *jaeger_query.JaegerQueryReader) http.Handler {
	hf := corsWrapper(conf, servicesHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func servicesHandler(reader *jaeger_query.JaegerQueryReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("into services")
		services, err := reader.GetServices(context.Background())
		if err != nil {
			log.Error("msg", fmt.Errorf("get services: %w", err))
			respondError(w, http.StatusInternalServerError, err, "fetching services")
			return
		}
		response := storage_v1.GetServicesResponse{Services: services}
		bSlice, err := response.Marshal()
		if err != nil {
			log.Error("msg", fmt.Errorf("marshal response: %w", err))
			respondProtoWithErr(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("sending response as", response)
		respondProto(w, http.StatusOK, bSlice)
	}
}
