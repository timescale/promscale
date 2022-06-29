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
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/oklog/run"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor/trace"
	dbMetrics "github.com/timescale/promscale/pkg/pgmodel/metrics/database"
	"github.com/timescale/promscale/pkg/rules"
	"github.com/timescale/promscale/pkg/telemetry"
	"github.com/timescale/promscale/pkg/thanos"
	"github.com/timescale/promscale/pkg/tracer"
	"github.com/timescale/promscale/pkg/util"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
)

var (
	startupError = fmt.Errorf("startup error")
	PromscaleID  uuid.UUID
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
	log.Info("msg", version.Info())

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
		dbMetricsCtx, stopDBMetrics := context.WithCancel(context.Background())
		defer stopDBMetrics()
		engine := dbMetrics.NewEngine(dbMetricsCtx, client.Connection)
		if err = engine.Run(); err != nil {
			log.Error("msg", "error running database metrics", "err", err.Error())
			return fmt.Errorf("error running database metrics: %w", err)
		}
	}

	rulesCtx, stopRuler := context.WithCancel(context.Background())
	defer stopRuler()
	manager, reloadRules, err := rules.NewManager(rulesCtx, prometheus.DefaultRegisterer, client, &cfg.RulesCfg)
	if err != nil {
		return fmt.Errorf("error creating rules manager: %w", err)
	}
	cfg.APICfg.Rules = manager

	var group run.Group
	group.Add(
		func() error {
			// Reload the rules before starting the rules-manager to ensure all rules are healthy.
			// Otherwise, we block the startup.
			if err = reloadRules(); err != nil {
				return fmt.Errorf("error reloading rules: %w", err)
			}
			log.Info("msg", "Started Rule-Manager")
			return manager.Run()
		}, func(error) {
			log.Info("msg", "Stopping Rule-Manager")
			stopRuler()
		},
	)

	jaegerQuery := query.New(client.QuerierConnection, &cfg.TracingCfg)

	router, err := api.GenerateRouter(&cfg.APICfg, &cfg.PromQLCfg, client, jaegerQuery, reloadRules)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	telemetryEngine := initTelemetryEngine(client)
	telemetryEngine.Start()
	defer telemetryEngine.Stop()

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
	ptraceotlp.RegisterServer(grpcServer, api.NewTraceServer(client))
	grpc_prometheus.Register(grpcServer)
	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
	)

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
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: time.Second * 30, // To mitigate Slowloris DDoS attack. Value is arbitrary picked
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
	signal.Notify(c, syscall.SIGINT, syscall.SIGHUP)
	group.Add(
		func() error {
			for {
				sig, open := <-c
				if !open {
					// Channel closed from error function. Let's shutdown.
					return nil
				}
				switch sig {
				case syscall.SIGINT:
					return nil
				case syscall.SIGHUP:
					if err := reloadRules(); err != nil {
						log.Error("msg", "error reloading rules", "err", err.Error())
						continue
					}
					log.Debug("msg", "success reloading rules")
				}
			}
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

func initTelemetryEngine(client *pgclient.Client) telemetry.Engine {
	t, err := telemetry.NewEngine(client.Connection, PromscaleID, client.Queryable())
	if err != nil {
		log.Debug("msg", "err creating telemetry engine", "err", err.Error())
		return t
	}
	if err := api.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for API telemetry", "err", err.Error())
	}
	if err := query.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for Jaeger-query telemetry", "err", err.Error())
	}
	if err := trace.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for Jaeger-ingest telemetry", "err", err.Error())
	}
	if err := rules.RegisterForTelemetry(t); err != nil {
		log.Error("msg", "error registering metrics for rules telemetry", "err", err.Error())
	}
	return t
}
