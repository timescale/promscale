// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/jaegerproxy"
	"google.golang.org/grpc"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Level:      hclog.Warn,
		Name:       "jaeger-plugin-proxy",
		JSONFormat: true,
	})

	var configAddr string
	flag.StringVar(&configAddr, "config", "", "Configuration file address")
	flag.Parse()

	bSlice, err := ioutil.ReadFile(configAddr)
	if err != nil {
		logger.Error("Invalid configuration file address, exiting.")
		os.Exit(1)
	}

	conf := new(jaegerproxy.ProxyConfig)
	if err = yaml.Unmarshal(bSlice, conf); err != nil {
		logger.Error("Unmarshalling configuration file contents", "err", err.Error())
		os.Exit(1)
	}

	//todo: Remove later
	logger.Warn("configuration", "conf", fmt.Sprintf("%v", conf))

	promscalePlugin, err := jaegerproxy.New(*conf, logger)
	if err != nil {
		logger.Error("could not start jaegerproxy: ", err)
		os.Exit(1)
	}
	defer promscalePlugin.Close()
	logger.Warn("starting to serve")
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: shared.Handshake,
		VersionedPlugins: map[int]plugin.PluginSet{
			1: map[string]plugin.Plugin{
				shared.StoragePluginIdentifier: &StorageGRPCPlugin{
					proxy: promscalePlugin,
				},
			},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
	//grpc.Serve(&shared.PluginServices{Store: promscalePlugin})
}

// Ensure plugin.GRPCPlugin API match.
var _ plugin.GRPCPlugin = (*StorageGRPCPlugin)(nil)

// StorageGRPCPlugin is the implementation of plugin.GRPCPlugin.
type StorageGRPCPlugin struct {
	plugin.Plugin
	// Concrete implementation, This is only used for plugins that are written in Go.
	proxy *jaegerproxy.Proxy
}

// GRPCServer implements plugin.GRPCPlugin. It is used by go-plugin to create a grpc plugin server.
func (p *StorageGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	storage_v1.RegisterSpanReaderPluginServer(s, p.proxy)
	storage_v1.RegisterSpanWriterPluginServer(s, p.proxy)
	storage_v1.RegisterArchiveSpanReaderPluginServer(s, p.proxy)
	storage_v1.RegisterArchiveSpanWriterPluginServer(s, p.proxy)
	storage_v1.RegisterPluginCapabilitiesServer(s, p.proxy)
	storage_v1.RegisterDependenciesReaderPluginServer(s, p.proxy)
	return nil
}

// GRPCClient implements plugin.GRPCPlugin. It is used by go-plugin to create a grpc plugin client.
func (*StorageGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
	p := shared.StorageGRPCPlugin{}
	return p.GRPCClient(ctx, broker, c)
}
