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
	"github.com/timescale/promscale/pkg/version"
	"google.golang.org/grpc"
)

const promLivenessCheck = time.Second

var (
	elector      *util.Elector
	startupError = fmt.Errorf("startup error")
)

func Run(cfg *Config) error {
	log.Info("msg", "Version:"+version.Promscale+"; Commit Hash: "+version.CommitHash)
	log.Info("config", util.MaskPassword(fmt.Sprintf("%+v", cfg)))

	if cfg.APICfg.ReadOnly {
		log.Info("msg", "Migrations disabled for read-only mode")
	}

	promMetrics := api.InitMetrics(cfg.PgmodelCfg.ReportInterval)

	client, err := CreateClient(cfg, promMetrics)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", util.MaskPassword(err.Error()))
		return startupError
	}

	if client == nil {
		return nil
	}

	defer client.Close()

	router, err := api.GenerateRouter(&cfg.APICfg, promMetrics, client, elector)
	if err != nil {
		log.Error("msg", "aborting startup due to error", "err", fmt.Sprintf("generate router: %s", err.Error()))
		return fmt.Errorf("generate router: %w", err)
	}

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.ListenAddr)

	srv := thanos.NewStorage(client.Queryable())
	grpcServer := grpc.NewServer()
	storepb.RegisterStoreServer(grpcServer, srv)

	go func() {
		log.Info("msg", fmt.Sprintf("Start listening %s", cfg.GrpcListenAddr))
		listener, err := net.Listen("tcp", cfg.GrpcListenAddr)
		if err != nil {
			log.Error("msg", "Listening grpc-server", "err", err)
			return
		}

		log.Info("msg", "Start thanos-store")
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("msg", "Starting grpc-server", "err", err)
			return
		}
	}()

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
