// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/thanos"
	"github.com/timescale/promscale/pkg/util"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const promLivenessCheck = time.Second

var (
	elector      *util.Elector
	startupError = fmt.Errorf("startup error")
)

func loggingUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	m, err := handler(ctx, req)
	if err != nil {
		log.Error("msg", "error in GRPC call", "err", err)
	}
	return m, err
}

func loggingStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	if err != nil {
		log.Error("msg", "error in GRPC call", "err", err)
	}
	return err
}

// WarnOnDeprecatedConfig prints warnings to the log for every deprecated configuration parameter which is used.
// Logging is not activated during the argument-parsing phase, so this work must be performed afterwards.
func WarnOnDeprecatedConfig(cfg *Config) {
	if cfg.HaGroupLockID != 0 {
		log.Warn("msg", "Deprecated flag 'leader-election-pg-advisory-lock-id' was used. Please use label-based leader election: https://github.com/timescale/promscale/blob/master/docs/high-availability/prometheus-HA.md#prometheus-leader-election-via-external-labels")
	}
	if len(cfg.APICfg.PromQLEnabledFeatureList) > 0 {
		log.Warn("msg", "Deprecated cli flag 'promql-enable-feature' used. Use 'enable-feature' instead.")
	}
}

func Run(cfg *Config) error {
	log.Info("msg", "Version:"+version.Promscale+"; Commit Hash: "+version.CommitHash)

	redacted := *cfg
	redacted.PgmodelCfg.Password = "****"
	redacted.PgmodelCfg.DbUri = "****"
	log.Info("config", fmt.Sprintf("%+v", redacted))

	if cfg.APICfg.ReadOnly {
		log.Info("msg", "Migrations disabled for read-only mode")
	} else {
		tput.InitWatcher(cfg.ThroughputInterval)
	}

	promMetrics := api.InitMetrics()
	client, err := CreateClient(cfg, promMetrics)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", err.Error())
		return startupError
	}

	if client == nil {
		return nil
	}

	defer client.Close()

	router, err := api.GenerateRouter(&cfg.APICfg, client, elector)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.ListenAddr)

	if len(cfg.ThanosStoreAPIListenAddr) > 0 {
		srv := thanos.NewStorage(client.Queryable())
		options := make([]grpc.ServerOption, 0)
		if cfg.TLSCertFile != "" {
			creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				log.Error("msg", "Setting up TLS credentials for Thanos StoreAPI failed", "err", err)
				return err
			}
			options = append(options, grpc.Creds(creds))
		}
		grpcServer := grpc.NewServer(options...)
		storepb.RegisterStoreServer(grpcServer, srv)

		go func() {
			log.Info("msg", fmt.Sprintf("Start listening for Thanos StoreAPI on %s", cfg.ThanosStoreAPIListenAddr))
			listener, err := net.Listen("tcp", cfg.ThanosStoreAPIListenAddr)
			if err != nil {
				log.Error("msg", "Listening for Thanos StoreAPI failed", "err", err)
				return
			}

			log.Info("msg", "Start thanos-store")
			if err := grpcServer.Serve(listener); err != nil {
				log.Error("msg", "Starting the Thanos store failed", "err", err)
				return
			}
		}()
	}

	if len(cfg.OTLPGRPCListenAddr) > 0 {
		options := []grpc.ServerOption{
			grpc.UnaryInterceptor(loggingUnaryInterceptor),
			grpc.StreamInterceptor(loggingStreamInterceptor),
		}
		if cfg.TLSCertFile != "" {
			creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
			if err != nil {
				log.Error("msg", "Setting up TLS credentials for OTLP GRPC server failed", "err", err)
				return err
			}
			options = append(options, grpc.Creds(creds))
		}
		grpcServer := grpc.NewServer(options...)
		otlpgrpc.RegisterTracesServer(grpcServer, api.NewTraceServer(client))

		queryPlugin := shared.StorageGRPCPlugin{
			Impl: query.New(client.QuerierConnection),
		}
		err := queryPlugin.GRPCServer(nil, grpcServer)
		if err != nil {
			log.Error("msg", "Creating jaeger query GRPC server failed", "err", err)
			return err
		}

		go func() {
			log.Info("msg", fmt.Sprintf("Start listening for OTLP GRPC server on %s", cfg.OTLPGRPCListenAddr))
			listener, err := net.Listen("tcp", cfg.OTLPGRPCListenAddr)
			if err != nil {
				log.Error("msg", "Listening for OTLP GRPC server failed", "err", err)
				return
			}

			log.Info("msg", "Start OTLP GRPC server")
			if err := grpcServer.Serve(listener); err != nil {
				log.Error("msg", "Starting the OTLP GRPC server failed", "err", err)
				return
			}
		}()
	}

	mux := http.NewServeMux()
	mux.Handle("/", router)

	if cfg.TLSCertFile != "" {
		err = http.ListenAndServeTLS(cfg.ListenAddr, cfg.TLSCertFile, cfg.TLSKeyFile, mux)
	} else {
		err = http.ListenAndServe(cfg.ListenAddr, mux)
	}

	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		return startupError
	}

	return nil
}
