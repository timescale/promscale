package cache

import (
	"flag"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/limits"
	"github.com/timescale/promscale/pkg/util"
)

// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

var (
	SeriesCacheMaxBytesMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "series_cache_max_bytes",
			Help:      "The maximum number of bytes of memory the series_cache will target",
		})
)

type Config struct {
	SeriesCacheInitialSize    uint64
	seriesCacheMemoryMaxFlag  limits.PercentageBytes
	SeriesCacheMemoryMaxBytes uint64

	MetricsCacheSize uint64
	LabelsCacheSize  uint64
}

var DefaultConfig = Config{
	SeriesCacheInitialSize:    DefaultSeriesCacheSize,
	SeriesCacheMemoryMaxBytes: 1000000,

	MetricsCacheSize: DefaultMetricCacheSize,
	LabelsCacheSize:  1000,
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	/* set defaults */
	cfg.seriesCacheMemoryMaxFlag.SetPercent(50)

	fs.Uint64Var(&cfg.MetricsCacheSize, "metrics-cache-size", DefaultMetricCacheSize, "Maximum number of metric names to cache.")
	fs.Uint64Var(&cfg.SeriesCacheInitialSize, "series-cache-initial-size", DefaultSeriesCacheSize, "Maximum number of series to cache.")
	fs.Uint64Var(&cfg.LabelsCacheSize, "labels-cache-size", 10000, "Maximum number of labels to cache.")
	fs.Var(&cfg.seriesCacheMemoryMaxFlag, "series-cache-max-bytes", "Target for amount of memory to use for the series cache. "+
		"Specified in bytes or as a percentage of the memory-target (e.g. 50%).")
	return cfg
}

func Validate(cfg *Config, lcfg limits.Config) error {
	percentage, bytes := cfg.seriesCacheMemoryMaxFlag.Get()
	if percentage > 0 {
		cfg.SeriesCacheMemoryMaxBytes = uint64(float64(lcfg.TargetMemoryBytes) * (float64(percentage) / 100.0))
	} else {
		cfg.SeriesCacheMemoryMaxBytes = bytes
	}
	SeriesCacheMaxBytesMetric.Set(float64(cfg.SeriesCacheMemoryMaxBytes))

	if cfg.SeriesCacheMemoryMaxBytes > lcfg.TargetMemoryBytes {
		return fmt.Errorf("The series-cache-max-bytes must be smaller than the memory-target")
	}

	return nil
}

func init() {
	prometheus.MustRegister(
		SeriesCacheMaxBytesMetric,
	)
}
