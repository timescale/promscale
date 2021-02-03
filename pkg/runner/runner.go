// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"fmt"
	"net/http"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/util"
	"github.com/timescale/promscale/pkg/version"
)

const (
	promLivenessCheck = time.Second
	schemaLockId      = 0x4D829C732AAFCEDE // chosen randomly.
)

var (
	elector      *util.Elector
	startupError = fmt.Errorf("startup error")
)

func Run(cfg *Config) error {
	log.Info("msg", "Version:"+version.Version+"; Commit Hash: "+version.CommitHash)
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
