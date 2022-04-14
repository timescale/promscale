// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/promql"
)

func NewEngine(logger log.Logger, queryTimeout, lookBackDelta, subqueryDefaultStepInterval time.Duration, maxSamples int64, enabledFeaturesMap map[string]struct{}) (*promql.Engine, error) {
	engineOpts := promql.EngineOpts{
		Logger:                   logger,
		Reg:                      prometheus.NewRegistry(),
		MaxSamples:               maxSamples,
		Timeout:                  queryTimeout,
		LookbackDelta:            lookBackDelta,
		NoStepSubqueryIntervalFn: func(int64) int64 { return durationMilliseconds(subqueryDefaultStepInterval) },
	}
	_, engineOpts.EnableAtModifier = enabledFeaturesMap["promql-at-modifier"]
	_, engineOpts.EnableNegativeOffset = enabledFeaturesMap["promql-negative-offset"]
	return promql.NewEngine(engineOpts), nil
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
