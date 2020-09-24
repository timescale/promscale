package query

import (
	"math"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/promql"
)

func NewEngine(logger log.Logger, queryTimeout time.Duration) *promql.Engine {
	return promql.NewEngine(
		promql.EngineOpts{
			Logger:                   logger,
			Reg:                      prometheus.NewRegistry(),
			MaxSamples:               math.MaxInt32,
			Timeout:                  queryTimeout,
			NoStepSubqueryIntervalFn: func(int64) int64 { return durationMilliseconds(1 * time.Minute) },
		},
	)
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
