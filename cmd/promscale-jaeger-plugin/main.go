package main

import (
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/timescale/promscale/cmd/promscale-jaeger-plugin/store"
)

func main() {
	store, err := store.NewStore()
	if err != nil {
		panic(err)
	}
	grpc.Serve(&shared.PluginServices{Store: store})
}
