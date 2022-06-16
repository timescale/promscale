// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rules

import (
	"flag"
	"fmt"
	"time"

	prometheus_config "github.com/prometheus/prometheus/config"

	"github.com/timescale/promscale/pkg/log"
)

const (
	DefaultNotificationQueueCapacity = 10000
	DefaultOutageTolerance           = time.Hour
	DefaultForGracePeriod            = time.Minute * 10
	DefaultResendDelay               = time.Minute
)

type Config struct {
	NotificationQueueCapacity int
	OutageTolerance           time.Duration
	ForGracePeriod            time.Duration
	ResendDelay               time.Duration
	PrometheusConfigAddress   string
	PrometheusConfig          *prometheus_config.Config
}

func (cfg *Config) ContainsRules() bool {
	return cfg.PrometheusConfig != nil && len(cfg.PrometheusConfig.RuleFiles) != 0
}

func (cfg *Config) ContainsAlertingConfig() bool {
	return cfg.PrometheusConfig != nil && len(cfg.PrometheusConfig.AlertingConfig.AlertmanagerConfigs) != 0
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.IntVar(&cfg.NotificationQueueCapacity, "metrics.alertmanager.notification-queue-capacity", DefaultNotificationQueueCapacity, "The capacity of the queue for pending Alertmanager notifications.")
	fs.DurationVar(&cfg.OutageTolerance, "metrics.rules.alert.for-outage-tolerance", DefaultOutageTolerance, "Max time to tolerate Promscale outage for restoring \"for\" state of alert.")
	fs.DurationVar(&cfg.ForGracePeriod, "metrics.rules.alert.for-grace-period", DefaultForGracePeriod, "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.")
	fs.DurationVar(&cfg.ResendDelay, "metrics.rules.alert.resend-delay", DefaultResendDelay, "Minimum amount of time to wait before resending an alert to Alertmanager.")
	fs.StringVar(&cfg.PrometheusConfigAddress, "metrics.rules.config-file", "", "Path to configuration file in Prometheus-format, containing `rule_files` and optional `alerting`, `global` fields. "+
		"For more details, see https://prometheus.io/docs/prometheus/latest/configuration/configuration/. "+
		"Note: If this is flag empty or `rule_files` is empty, Promscale rule-manager will not start. If `alertmanagers` is empty, alerting will not be initialized.")
	return cfg
}

func Validate(cfg *Config) error {
	if cfg.PrometheusConfigAddress == "" {
		cfg.PrometheusConfig = &prometheus_config.DefaultConfig
		return nil
	}
	promCfg, err := prometheus_config.LoadFile(cfg.PrometheusConfigAddress, false, true, log.GetLogger())
	if err != nil {
		return fmt.Errorf("error loading Prometheus configuration file: %w", err)
	}
	cfg.PrometheusConfig = promCfg
	return nil
}
