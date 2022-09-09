package store

import (
	"flag"
	"time"
)

const (
	DefaultMaxTraceDuration = time.Hour
)

type Config struct {
	MaxTraceDuration    time.Duration
	StreamingSpanWriter bool
}

var DefaultConfig = Config{
	MaxTraceDuration:    DefaultMaxTraceDuration,
	StreamingSpanWriter: true,
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.DurationVar(&cfg.MaxTraceDuration, "tracing.max-trace-duration", DefaultMaxTraceDuration, "Maximum duration of any trace in the system. This parameter is used to optimize queries.")
	fs.BoolVar(&cfg.StreamingSpanWriter, "tracing.streaming-span-writer", true, "StreamingSpanWriter for remote Jaeger grpc store.")
	return cfg
}

func Validate(cfg *Config) error {
	return nil
}
