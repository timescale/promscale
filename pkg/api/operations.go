// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package api

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/log"
	jaeger_query "github.com/timescale/promscale/pkg/plugin/jaeger-query"
)

func Operations(conf *Config, reader *jaeger_query.JaegerQueryReader) http.Handler {
	hf := corsWrapper(conf, operationsHandler(reader))
	return gziphandler.GzipHandler(hf)
}

func operationsHandler(reader *jaeger_query.JaegerQueryReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("operations request")
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", fmt.Errorf("reading body: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		var request storage_v1.GetOperationsRequest
		if err = request.Unmarshal(b); err != nil {
			log.Error("msg", fmt.Errorf("unmarshalling request: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		response, err := reader.GetOperations(context.Background(), request)
		if err != nil {
			log.Error("msg", fmt.Errorf("get operations: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		b, err = response.Marshal()
		if err != nil {
			log.Error("msg", fmt.Errorf("marshal operations: %w", err))
			respondWithStatusOnly(w, http.StatusInternalServerError)
			return
		}
		fmt.Println("sending operations response as", response)
		respondWithByteSlice(w, http.StatusOK, b)
	}
}
