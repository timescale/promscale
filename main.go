package main

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote"
)

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

type reader interface {
	Read(req *remote.ReadRequest) (*remote.ReadResponse, error)
	Name() string
}

type config struct {
	remoteTimeout time.Duration
	listenAddr    string
	telemetryPath string
}

func parseFlags() *config {
	cfg := config{}

	return &cfg
}

func main() {
	cfg := parseFlags()
	http.Handle(cfg.telemetryPath, prometheus.Handler())

	writers, readers := buildClients(cfg)
	serve(cfg.listenAddr, writers, readers)
}
