package main

import (
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/timescale/promscale/cmd/promscale-jaeger-plugin/connector"
)

func main() {
	reader, err := connector.NewStore()
	if err != nil {
		panic(err)
	}
	grpc.Serve(&shared.PluginServices{Store: store})
}
