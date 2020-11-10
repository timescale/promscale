// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

// Package log creates logs in the same way as Prometheus, while ignoring errors
package log

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

var (
	// Application wide logger
	logger log.Logger = log.NewNopLogger()

	// logger timestamp format
	timestampFormat = log.TimestampFormat(
		func() time.Time { return time.Now().UTC() },
		"2006-01-02T15:04:05.000Z07:00",
	)
)

// Config represents a logger configuration used upon initialization.
type Config struct {
	Level  string
	Format string
}

// ParseFlags parses the configuration flags for logging.
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.Level, "log-level", "debug", "Log level to use from [ 'error', 'warn', 'info', 'debug' ].")
	flag.StringVar(&cfg.Format, "log-format", "logfmt", "The log format to use [ 'logfmt', 'json' ].")
	return cfg
}

// Init starts logging given the configuration. By default, it uses logfmt format
// and minimum logging level.
func Init(cfg Config) error {
	var l log.Logger
	switch cfg.Format {
	case "logfmt", "":
		l = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	case "json":
		l = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	default:
		return fmt.Errorf("unrecognized log format %s", cfg.Format)
	}

	logLevelOption, err := parseLogLevel(cfg.Level)
	if err != nil {
		return err
	}

	l = level.NewFilter(l, logLevelOption)
	// NOTE: we add a level of indirection with our logging functions,
	//       so we need additional caller depth
	logger = log.With(l, "ts", timestampFormat, "caller", log.Caller(4))
	return nil
}

func GetLogger() log.Logger {
	return logger
}

// Debug logs a DEBUG level message, ignoring logging errors
func Debug(keyvals ...interface{}) {
	_ = level.Debug(logger).Log(keyvals...)
}

// Info logs an INFO level message, ignoring logging errors
func Info(keyvals ...interface{}) {
	_ = level.Info(logger).Log(keyvals...)
}

// Warn logs a WARN level message, ignoring logging errors
func Warn(keyvals ...interface{}) {
	_ = level.Warn(logger).Log(keyvals...)
}

// Error logs an ERROR level message, ignoring logging errors
func Error(keyvals ...interface{}) {
	_ = level.Error(logger).Log(keyvals...)
}

// Fatal logs an ERROR level message and exits
func Fatal(keyvals ...interface{}) {
	_ = level.Error(logger).Log(keyvals...)
	os.Exit(1)
}

func parseLogLevel(logLevel string) (level.Option, error) {
	switch logLevel {
	case "debug":
		return level.AllowDebug(), nil
	case "info":
		return level.AllowInfo(), nil
	case "warn":
		return level.AllowWarn(), nil
	case "error":
		return level.AllowError(), nil
	default:
		return nil, fmt.Errorf("unrecognized log level %q", logLevel)
	}
}
