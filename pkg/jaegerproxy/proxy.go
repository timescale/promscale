// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaegerproxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const DefaultTimeout = time.Duration(5 * time.Second)

type ProxyConfig struct {
	TLS                bool          `yaml:"promscale-tls,omitempty"`
	CaFile             string        `yaml:"promscale-cafile,omitempty"`
	ServerAddr         string        `yaml:"promscale-server,omitempty"`
	ServerHostOverride string        `yaml:"promscale-server-host-override,omitempty"`
	ConnectTimeout     time.Duration `yaml:"timeout,omitempty"`
}

type Proxy struct {
	config                  ProxyConfig
	logger                  hclog.Logger
	conn                    *grpc.ClientConn
	spanReaderClient        storage_v1.SpanReaderPluginClient
	spanWriterClient        storage_v1.SpanWriterPluginClient
	dependencyReaderClient  storage_v1.DependenciesReaderPluginClient
	capClient               storage_v1.PluginCapabilitiesClient
	archiveSpanReaderClient storage_v1.ArchiveSpanReaderPluginClient
	archiveSpanWriterClient storage_v1.ArchiveSpanWriterPluginClient
}

func New(config ProxyConfig, logger hclog.Logger) (*Proxy, error) {
	var opts []grpc.DialOption
	var err error
	if config.TLS {
		if config.CaFile == "" {
			return nil, fmt.Errorf("ca file is required with TLS")
		}
		creds, err := credentials.NewClientTLSFromFile(config.CaFile, config.ServerHostOverride)
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = DefaultTimeout
	}

	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, config.ServerAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	return &Proxy{
		config:                  config,
		logger:                  logger,
		conn:                    conn,
		spanReaderClient:        storage_v1.NewSpanReaderPluginClient(conn),
		dependencyReaderClient:  storage_v1.NewDependenciesReaderPluginClient(conn),
		capClient:               storage_v1.NewPluginCapabilitiesClient(conn),
		archiveSpanReaderClient: storage_v1.NewArchiveSpanReaderPluginClient(conn),
		archiveSpanWriterClient: storage_v1.NewArchiveSpanWriterPluginClient(conn),
	}, nil
}

func (p *Proxy) Close() {
	p.conn.Close()
}

func (p *Proxy) GetDependencies(ctx context.Context, r *storage_v1.GetDependenciesRequest) (*storage_v1.GetDependenciesResponse, error) {
	return p.dependencyReaderClient.GetDependencies(ctx, r)
}

func (p *Proxy) WriteSpan(ctx context.Context, r *storage_v1.WriteSpanRequest) (*storage_v1.WriteSpanResponse, error) {
	//TODO: figure out what to do here if we want the jaeger-all-in-one to work. Fine if using jaeger-query
	return p.spanWriterClient.WriteSpan(ctx, r)
}

func (p *Proxy) GetTrace(r *storage_v1.GetTraceRequest, stream storage_v1.SpanReaderPlugin_GetTraceServer) error {
	client, err := p.spanReaderClient.GetTrace(stream.Context(), r)
	if err != nil {
		return err
	}
	for {
		m, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(m); err != nil {
			return err
		}
	}
}

func (p *Proxy) GetServices(ctx context.Context, r *storage_v1.GetServicesRequest) (*storage_v1.GetServicesResponse, error) {
	return p.spanReaderClient.GetServices(ctx, r)
}

func (p *Proxy) GetOperations(
	ctx context.Context,
	r *storage_v1.GetOperationsRequest,
) (*storage_v1.GetOperationsResponse, error) {
	return p.spanReaderClient.GetOperations(ctx, r)
}

func (p *Proxy) FindTraces(r *storage_v1.FindTracesRequest, stream storage_v1.SpanReaderPlugin_FindTracesServer) error {
	client, err := p.spanReaderClient.FindTraces(stream.Context(), r)
	if err != nil {
		return err
	}
	for {
		m, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(m); err != nil {
			return err
		}
	}
}

func (p *Proxy) FindTraceIDs(ctx context.Context, r *storage_v1.FindTraceIDsRequest) (*storage_v1.FindTraceIDsResponse, error) {
	return p.spanReaderClient.FindTraceIDs(ctx, r)
}

func (p *Proxy) Capabilities(ctx context.Context, r *storage_v1.CapabilitiesRequest) (*storage_v1.CapabilitiesResponse, error) {
	return p.capClient.Capabilities(ctx, r)
}

func (p *Proxy) GetArchiveTrace(r *storage_v1.GetTraceRequest, stream storage_v1.ArchiveSpanReaderPlugin_GetArchiveTraceServer) error {
	client, err := p.archiveSpanReaderClient.GetArchiveTrace(stream.Context(), r)
	if err != nil {
		return err
	}
	for {
		m, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = stream.Send(m); err != nil {
			return err
		}
	}
}

func (p *Proxy) WriteArchiveSpan(ctx context.Context, r *storage_v1.WriteSpanRequest) (*storage_v1.WriteSpanResponse, error) {
	return p.archiveSpanWriterClient.WriteArchiveSpan(ctx, r)
}
