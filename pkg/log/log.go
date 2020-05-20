// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

// Package log creates logs in the same way as Prometheus, while ignoring errors
package log

import (
	"fmt"
	"os"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

var (
	// Application wide logger
	logger log.Logger

	// logger timestamp format
	timestampFormat = log.TimestampFormat(
		func() time.Time { return time.Now().UTC() },
		"2006-01-02T15:04:05.000Z07:00",
	)
)

// Init starts logging given the minimum log level
func Init(logLevel string) error {
	var l log.Logger
	l = log.NewJSONLogger(log.NewSyncWriter(os.Stderr))
	logLevelOption, err := parseLogLevel(logLevel)
	if err != nil {
		return err
	}

	l = level.NewFilter(l, logLevelOption)
	logger = log.With(l, "ts", timestampFormat, "caller", log.Caller(4))
	return nil
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

// CustomCacheLogger is a custom logger used for transforming cache logs
// so that they conform the our logging setup. It also implements the
// bigcache.Logger interface.
type CustomCacheLogger struct{}

// Printf sends the log in the debug stream of the logger.
func (c *CustomCacheLogger) Printf(format string, v ...interface{}) {
	_ = level.Debug(logger).Log("msg", fmt.Sprintf(format, v...))
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
