package query

import (
	"math"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/timescale-prometheus/pkg/promql"
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
