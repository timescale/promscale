package query

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/timescale/promscale/pkg/log"
)

const (
	DefaultQueryTimeout         = time.Minute * 2
	DefaultLookBackDelta        = time.Minute * 5
	DefaultSubqueryStepInterval = time.Minute
	DefaultMaxSamples           = 50000000
)

type CommaSeparatedList []string

func (v *CommaSeparatedList) String() string {
	if v != nil {
		return strings.Join(*v, ",")
	}
	return ""
}

func (v *CommaSeparatedList) Set(s string) error {
	*v = strings.Split(s, ",")
	return nil
}

type Config struct {
	EnabledFeatureMap           map[string]struct{}
	PromscaleEnabledFeatureList CommaSeparatedList

	MaxQueryTimeout      time.Duration
	SubQueryStepInterval time.Duration // Default step interval value if the user has not provided.
	LookBackDelta        time.Duration
	MaxSamples           int64
	MaxPointsPerTs       int64
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.Var(&cfg.PromscaleEnabledFeatureList, "enable-feature", "Enable beta/experimental features as a comma-separated list. Currently the following values can be passed: promql-at-modifier, promql-negative-offset")

	fs.DurationVar(&cfg.MaxQueryTimeout, "metrics.promql.query-timeout", DefaultQueryTimeout, "Maximum time a query may take before being aborted. This option sets both the default and maximum value of the 'timeout' parameter in "+
		"'/api/v1/query.*' endpoints.")
	fs.DurationVar(&cfg.SubQueryStepInterval, "metrics.promql.default-subquery-step-interval", DefaultSubqueryStepInterval, "Default step interval to be used for PromQL subquery evaluation. "+
		"This value is used if the subquery does not specify the step value explicitly. Example: <metric_name>[30m:]. Note: in Prometheus this setting is set by the evaluation_interval option.")
	fs.DurationVar(&cfg.LookBackDelta, "metrics.promql.lookback-delta", DefaultLookBackDelta, "Maximum lookback duration for retrieving metrics during expression evaluations and federation.")
	fs.Int64Var(&cfg.MaxSamples, "metrics.promql.max-samples", DefaultMaxSamples, "Maximum number of samples a single "+
		"query can load into memory. Note that queries will fail if they try to load more samples than this into memory, "+
		"so this also limits the number of samples a query can return.")
	fs.Int64Var(&cfg.MaxPointsPerTs, "metrics.promql.max-points-per-ts", 11000, "Maximum number of points per time-series in a query-range request. "+
		"This calculation is an estimation, that happens as (start - end)/step where start and end are the 'start' and 'end' timestamps of the query_range.")
	return cfg
}

func Validate(cfg *Config) error {
	cfg.EnabledFeatureMap = make(map[string]struct{})
	for _, f := range cfg.PromscaleEnabledFeatureList {
		switch f {
		case "promql-at-modifier", "promql-negative-offset":
			cfg.EnabledFeatureMap[f] = struct{}{}
		case "tracing":
			log.Error("msg", "tracing feature is now on by default, no need to use it with --enable-feature flag")
		default:
			return fmt.Errorf("invalid feature: %s", f)
		}
	}
	return nil
}
