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

func Init(logLevel string) {
	allowedLevel := promlog.AllowedLevel{}
	allowedLevel.Set(logLevel)
	logger = promlog.New(allowedLevel)
}

func Debug(keyvals ...interface{}) {
	level.Debug(logger).Log(keyvals...)
}

func Info(keyvals ...interface{}) {
	level.Info(logger).Log(keyvals...)
}

func Warn(keyvals ...interface{}) {
	level.Warn(logger).Log(keyvals...)
}

func Error(keyvals ...interface{}) {
	level.Error(logger).Log(keyvals...)
}
