package query

import (
	"flag"
	"time"
)

const (
	DefaultMaxTraceDuration = time.Hour
)

type Config struct {
	MaxTraceDuration time.Duration
}

var DefaultConfig = Config{
	MaxTraceDuration: DefaultMaxTraceDuration,
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.DurationVar(&cfg.MaxTraceDuration, "tracing.max-trace-duration", DefaultMaxTraceDuration, "Maximum duration of any trace in the system. This parameter is used to optimize queries.")
	return cfg
}

func Validate(cfg *Config) error {
	return nil
}
