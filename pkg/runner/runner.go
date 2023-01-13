// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package runner

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/run"
	"github.com/timescale/promscale/pkg/vacuum"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/google/uuid"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/store/storepb"

	"github.com/timescale/promscale/pkg/api"
	jaegerStore "github.com/timescale/promscale/pkg/jaeger/store"
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
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("msg", "error in GRPC call", "err", err)
	}
	return m, err
}

func loggingStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if ok && (st.Code() == codes.NotFound || st.Code() == codes.Canceled) {
		return err
	}

	log.Error("msg", "error in GRPC call", "err", err)
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
	client, err := CreateClient(prometheus.DefaultRegisterer, cfg)
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

	if util.IsTimescaleDBInstalled(client.ReadOnlyConnection()) {
		dbMetricsCtx, stopDBMetrics := context.WithCancel(context.Background())
		defer stopDBMetrics()
		engine := dbMetrics.NewEngine(dbMetricsCtx, client.ReadOnlyConnection())
		if err = engine.Run(); err != nil {
			log.Error("msg", "error running database metrics", "err", err.Error())
			return fmt.Errorf("error running database metrics: %w", err)
		}
	}

	var (
		group         run.Group
		rulesReloader func() error
	)
	if !cfg.APICfg.ReadOnly {
		rulesCtx, stopRuler := context.WithCancel(context.Background())
		defer stopRuler()
		manager, reloadRules, err := rules.NewManager(rulesCtx, prometheus.DefaultRegisterer, client, &cfg.RulesCfg)
		if err != nil {
			log.Error("msg", "error creating rules manager", "err", err.Error())
			return fmt.Errorf("error creating rules manager: %w", err)
		}
		cfg.APICfg.Rules = manager
		rulesReloader = reloadRules

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
	}

	jaegerStore := jaegerStore.New(client.ReadOnlyConnection(), client.Inserter(), &cfg.TracingCfg)

	authWrapper := func(h http.Handler) http.Handler {
		return cfg.AuthConfig.AuthHandler(h)
	}

	router, err := api.GenerateRouter(&cfg.APICfg, &cfg.PromQLCfg, client, jaegerStore, authWrapper, rulesReloader)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	telemetryEngine, err := initTelemetryEngine(client)
	if err != nil {
		log.Debug("msg", "error starting telemetry engine", "error", err.Error())
	} else {
		telemetryEngine.Start()
		defer telemetryEngine.Stop()
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

	queryPlugin := shared.StorageGRPCPlugin{
		Impl: jaegerStore,
	}
	if cfg.TracingCfg.StreamingSpanWriter {
		queryPlugin.StreamImpl = jaegerStore
		log.Info("msg", "Jaeger StreamingSpanWriter is enabled")
	} else {
		log.Info("msg", "Jaeger StreamingSpanWriter is disabled")
	}
	if err = queryPlugin.GRPCServer(nil, grpcServer); err != nil {
		log.Error("msg", "Creating jaeger query GRPC server failed", "err", err)
		return err
	}

	grpc_prometheus.EnableHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
	)
	grpc_prometheus.Register(grpcServer)

	group.Add(
		func() error {
			listener, err := net.Listen("tcp", cfg.TracingGRPCListenAddr)
			if err != nil {
				log.Error("msg", "Failed creating server listener for Jaeger and OTEL traces", "err", err)
				return err
			}
			log.Info("msg", "Started GRPC server for Jaeger and OTEL traces", "listening-port", cfg.TracingGRPCListenAddr)
			return grpcServer.Serve(listener)
		}, func(error) {
			log.Info("msg", "Stopping GRPC server for Jaeger and OTEL traces")
			grpcServer.Stop()
		},
	)

	if !cfg.VacuumCfg.Disable {
		ve, err := vacuum.NewEngine(client.MaintenanceConnection(), cfg.VacuumCfg.RunFrequency, cfg.VacuumCfg.MaxParallelism)
		if err != nil {
			log.Error("msg", "Failed to create vacuum engine", "err", err)
			return err
		}
		group.Add(
			func() error {
				log.Info("msg", "Starting vacuum engine")
				ve.Start()
				return nil
			}, func(err error) {
				log.Info("msg", "Stopping vacuum engine")
				ve.Stop()
			},
		)
	}

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
					if err := rulesReloader(); err != nil {
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

func initTelemetryEngine(client *pgclient.Client) (telemetry.Engine, error) {
	t, err := telemetry.NewEngine(client.MaintenanceConnection(), PromscaleID, client.Queryable())
	if err != nil {
		log.Debug("msg", "err creating telemetry engine", "err", err.Error())
		return nil, err
	}
	if err := api.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for API telemetry", "err", err.Error())
	}
	if err := jaegerStore.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for Jaeger-query telemetry", "err", err.Error())
	}
	if err := trace.RegisterTelemetryMetrics(t); err != nil {
		log.Error("msg", "error registering metrics for Jaeger-ingest telemetry", "err", err.Error())
	}
	if err := rules.RegisterForTelemetry(t); err != nil {
		log.Error("msg", "error registering metrics for rules telemetry", "err", err.Error())
	}
	return t, nil
}
