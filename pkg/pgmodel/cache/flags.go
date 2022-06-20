// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package cache

import (
	"flag"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/limits"
	"github.com/timescale/promscale/pkg/util"
)

var (
	SeriesCacheMaxBytesMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Name:      "series_cache_max_bytes",
			Help:      "The target for the maximum amount of memory the series_cache can use in bytes.",
		})
)

type Config struct {
	SeriesCacheInitialSize    uint64
	seriesCacheMemoryMaxFlag  limits.PercentageAbsoluteBytesFlag
	SeriesCacheMemoryMaxBytes uint64

	MetricsCacheSize        uint64
	LabelsCacheSize         uint64
	ExemplarKeyPosCacheSize uint64
	InvertedLabelsCacheSize uint64
}

var DefaultConfig = Config{
	SeriesCacheInitialSize:    DefaultSeriesCacheSize,
	SeriesCacheMemoryMaxBytes: 1000000,

	MetricsCacheSize:        DefaultMetricCacheSize,
	LabelsCacheSize:         DefaultLabelsCacheSize,
	ExemplarKeyPosCacheSize: DefaultExemplarKeyPosCacheSize,
	InvertedLabelsCacheSize: DefaultInvertedLabelsCacheSize,
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	/* set defaults */
	cfg.seriesCacheMemoryMaxFlag.SetPercent(50)

	fs.Uint64Var(&cfg.MetricsCacheSize, "metrics.cache.metrics.size", DefaultMetricCacheSize, "Maximum number of metric names to cache.")
	fs.Uint64Var(&cfg.SeriesCacheInitialSize, "metrics.cache.series.initial-size", DefaultSeriesCacheSize, "Maximum number of series to cache.")
	fs.Uint64Var(&cfg.LabelsCacheSize, "metrics.cache.labels.size", DefaultLabelsCacheSize, "Maximum number of labels to cache.")
	fs.Uint64Var(&cfg.ExemplarKeyPosCacheSize, "metrics.cache.exemplar.size", DefaultExemplarKeyPosCacheSize, "Maximum number of exemplar metrics key-position to cache. "+
		"It has one-to-one mapping with number of metrics that have exemplar, as key positions are saved per metric basis.")
	fs.Var(&cfg.seriesCacheMemoryMaxFlag, "metrics.cache.series.max-bytes", "Initial number of elements in the series cache. "+
		"Specified in bytes or as a percentage of the memory-target (e.g. 50%).")
	fs.Uint64Var(&cfg.InvertedLabelsCacheSize, "metrics.cache.inverted-labels.size", DefaultInvertedLabelsCacheSize, "Maximum number of label-ids to cache. This helps increase ingest performance.")
	return cfg
}

func Validate(cfg *Config, lcfg limits.Config) error {
	kind, value := cfg.seriesCacheMemoryMaxFlag.Get()
	switch kind {
	case limits.Percentage:
		cfg.SeriesCacheMemoryMaxBytes = uint64(float64(lcfg.TargetMemoryBytes) * (float64(value) / 100.0))
	case limits.Absolute:
		cfg.SeriesCacheMemoryMaxBytes = value
	default:
		return fmt.Errorf("series-cache-max-bytes flag has unknown kind")
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
