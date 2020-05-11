package promql

import (
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"math"
	"time"
)

func NewEngine(logger log.Logger, queryTimeout time.Duration) *promql.Engine {
	return promql.NewEngine(
		promql.EngineOpts{
			Logger:     logger,
			Reg:        prometheus.NewRegistry(),
			MaxSamples: math.MaxInt32,
			Timeout:    queryTimeout,
		},
	)
}
