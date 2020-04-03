package log

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/promlog"
)

var (
	// Application wide logger
	logger log.Logger
)

func Init(logLevel string) error {
	allowedLevel := promlog.AllowedLevel{}
	err := allowedLevel.Set(logLevel)
	if err != nil {
		return err
	}

	config := promlog.Config{
		Level:  &allowedLevel,
		Format: &promlog.AllowedFormat{},
	}

	logger = promlog.New(&config)
	return nil
}

func Debug(keyvals ...interface{}) {
	_ = level.Debug(logger).Log(keyvals...)
}

func Info(keyvals ...interface{}) {
	_ = level.Info(logger).Log(keyvals...)
}

func Warn(keyvals ...interface{}) {
	_ = level.Warn(logger).Log(keyvals...)
}

func Error(keyvals ...interface{}) {
	_ = level.Error(logger).Log(keyvals...)
}
