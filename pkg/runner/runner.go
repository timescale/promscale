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
	"os"
	"os/signal"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/oklog/run"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"

	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/log"
	dbMetrics "github.com/timescale/promscale/pkg/pgmodel/metrics/database"
	"github.com/timescale/promscale/pkg/thanos"
	"github.com/timescale/promscale/pkg/tracer"
	"github.com/timescale/promscale/pkg/util"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
)

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

	if cfg.TracerCfg.CanExportTraces() {
		tp, err := tracer.InitProvider(&cfg.TracerCfg)
		if err != nil {
			log.Error("msg", "aborting startup due to tracer provider error", "err", err.Error())
			return startupError
		}

		// Register our TracerProvider as the global so any imported
		// instrumentation in the future will default to using it.
		otel.SetTracerProvider(tp)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Cleanly shutdown and flush telemetry when the application exits.
		defer func(ctx context.Context) {
			// Do not make the application hang when it is shutdown.
			ctx, cancel = context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			if err := tp.Shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}(ctx)
	}

	api.InitMetrics()
	client, err := CreateClient(cfg)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", err.Error())
		return startupError
	}

	if client == nil {
		return nil
	}
	defer client.Close()

	if cfg.StartupOnly {
		log.Info("msg", "Promscale in startup-only mode (using flag `-startup.only`), exiting post startup...")
		return nil
	}

	if util.IsTimescaleDBInstalled(client.Connection) {
		engine := dbMetrics.NewEngine(client.Connection)
		engine.Start()
	}

	router, err := api.GenerateRouter(&cfg.APICfg, client, elector)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	err = api.RegisterMetricsForTelemetry(client.TelemetryEngine)
	if err != nil {
		log.Error("msg", "error registering metrics for telemetry", "err", err.Error())
		return fmt.Errorf("error registering metrics for telemetry: %w", err)
	}

	var group run.Group
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

		group.Add(
			func() error {
				listener, err := net.Listen("tcp", cfg.ThanosStoreAPIListenAddr)
				if err != nil {
					log.Error("msg", "Listening for Thanos StoreAPI failed", "err", err)
					return err
				}
				log.Info("msg", "Started Thanos StoreAPI GRPC server", "listening-port", cfg.ThanosStoreAPIListenAddr)
				return grpcServer.Serve(listener)
			}, func(error) {
				log.Info("msg", "Stopping Thanos StoreAPI GRPC server")
				grpcServer.Stop()
			},
		)
	}

	options := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor, grpc_prometheus.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor, grpc_prometheus.StreamServerInterceptor),
	}
	if cfg.TLSCertFile != "" {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			log.Error("msg", "Setting up TLS credentials for OpenTelemetry GRPC server failed", "err", err)
			return err
		}
		options = append(options, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(options...)
	otlpgrpc.RegisterTracesServer(grpcServer, api.NewTraceServer(client))
	grpc_prometheus.Register(grpcServer)
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
	)

	jaegerQuery, err := query.New(client.QuerierConnection, client.TelemetryEngine)
	if err != nil {
		log.Error("msg", "Creating jaeger query failed", "err", err)
		return err
	}

	queryPlugin := shared.StorageGRPCPlugin{
		Impl: jaegerQuery,
	}
	if err = queryPlugin.GRPCServer(nil, grpcServer); err != nil {
		log.Error("msg", "Creating jaeger query GRPC server failed", "err", err)
		return err
	}

	group.Add(
		func() error {
			listener, err := net.Listen("tcp", cfg.OTLPGRPCListenAddr)
			if err != nil {
				log.Error("msg", "Listening for OpenTelemetry OTLP GRPC server failed", "err", err)
				return err
			}
			log.Info("msg", "Started OpenTelemetry OTLP GRPC server", "listening-port", cfg.OTLPGRPCListenAddr)
			return grpcServer.Serve(listener)
		}, func(error) {
			log.Info("msg", "Stopping OpenTelemetry OTLP GRPC server")
			grpcServer.Stop()
		},
	)

	mux := http.NewServeMux()
	mux.Handle("/", router)

	server := http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}
	group.Add(
		func() error {
			var err error
			log.Info("msg", "Started Prometheus remote-storage HTTP server", "listening-port", cfg.ListenAddr)
			if cfg.TLSCertFile != "" {
				err = server.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
			} else {
				err = server.ListenAndServe()
			}
			return err
		}, func(error) {
			log.Info("msg", "Stopping Prometheus remote-storage HTTP server")
			err = server.Shutdown(context.Background())
			if err != nil {
				log.Error("msg", "unable to shutdown Prometheus remote-storage HTTP server", "err", err.Error())
			}
		},
	)

	// Listen to OS interrupt signals.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	group.Add(
		func() error {
			<-c
			return nil
		}, func(err error) {
			close(c)
		},
	)

	err = group.Run()
	if err != nil {
		log.Error("msg", "Execution failure, stopping Promscale", "err", err)
	}
	return err
}
