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

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
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
	"github.com/timescale/promscale/pkg/telemetry"
	"github.com/timescale/promscale/pkg/thanos"
	"github.com/timescale/promscale/pkg/tracer"
	"github.com/timescale/promscale/pkg/util"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
)

var (
	elector     *util.Elector
	PromscaleID uuid.UUID

	startupError = fmt.Errorf("startup error")
)

func init() {
	// PromscaleID must always be generated on start, so that it remains constant throughout the lifecycle.
	PromscaleID = uuid.New()
}

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

	if cfg.TracerCfg.JaegerCollectorEndpoint != "" {
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

	router, err := api.GenerateRouter(&cfg.APICfg, client, elector)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.ListenAddr)

	var telemetryEngine telemetry.Engine

	engine, err := telemetry.NewEngine(client.Connection, PromscaleID, client.Queryable())
	if err != nil {
		log.Error("msg", "aborting startup due to error in setting up telemetry-engine", "err", err.Error())
		return fmt.Errorf("creating telemetry-engine: %w", err)
	}

	if engine != nil {
		telemetryEngine = engine
	} else {
		telemetryEngine = telemetry.NewNoopEngine()
	}

	telemetryEngine.Start()
	defer telemetryEngine.Stop()

	err = api.RegisterMetricsForTelemetry(telemetryEngine)
	if err != nil {
		log.Error("msg", "error registering metrics for telemetry", "err", err.Error())
		return fmt.Errorf("error registering metrics for telemetry: %w", err)
	}

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
			grpc.ChainUnaryInterceptor(loggingUnaryInterceptor, grpc_prometheus.UnaryServerInterceptor),
			grpc.ChainStreamInterceptor(loggingStreamInterceptor, grpc_prometheus.StreamServerInterceptor),
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
		grpc_prometheus.Register(grpcServer)
		grpc_prometheus.EnableHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		)

		jaegerQuery, err := query.New(client.QuerierConnection, telemetryEngine)
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
