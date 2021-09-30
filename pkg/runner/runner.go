// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"fmt"
	"net"
	"net/http"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/thanos-io/thanos/pkg/store/storepb"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/thanos"
	"github.com/timescale/promscale/pkg/util"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"github.com/timescale/promscale/pkg/version"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"google.golang.org/grpc"
)

const promLivenessCheck = time.Second

var (
	elector      *util.Elector
	startupError = fmt.Errorf("startup error")
)

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
		grpcServer := grpc.NewServer()
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
		grpcServer := grpc.NewServer()
		otlpgrpc.RegisterTracesServer(grpcServer, api.NewTraceServer(client))

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
