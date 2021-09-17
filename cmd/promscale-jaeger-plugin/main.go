package main

import (
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/timescale/promscale/cmd/promscale-jaeger-plugin/plugin"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Name:       "promscale-jaeger-plugin",
		JSONFormat: true,
	})
	promscalePlugin := plugin.New("http://localhost:9201", logger, time.Second*10)
	grpc.Serve(&shared.PluginServices{Store: promscalePlugin})
}
