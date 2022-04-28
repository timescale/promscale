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
	defaultNotificationQueueCapacity = 10000
	defaultOutageTolerance           = time.Hour
	defaultForGracePeriod            = time.Minute * 10
	defaultResendDelay               = time.Minute
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
	if cfg.PrometheusConfig == nil {
		return false
	}
	promCfg := cfg.PrometheusConfig
	if len(promCfg.RuleFiles) == 0 {
		log.Info("msg", "Rules files not found in the given Prometheus configuration file. Both rule-manager and alerting will not be initialized")
		return false
	}

	if promCfg.AlertingConfig.AlertRelabelConfigs == nil && promCfg.AlertingConfig.AlertmanagerConfigs == nil {
		// This is true when alerting config is not applied.
		log.Info("msg", "Alerting configuration not present in the given Prometheus configuration file. Alerting will not be initialized")
	}
	return true
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.IntVar(&cfg.NotificationQueueCapacity, "metrics.alertmanager.notification-queue-capacity", defaultNotificationQueueCapacity, "The capacity of the queue for pending Alertmanager notifications.")
	fs.DurationVar(&cfg.OutageTolerance, "metrics.rules.alert.for-outage-tolerance", defaultOutageTolerance, "Max time to tolerate Promscale outage for restoring \"for\" state of alert.")
	fs.DurationVar(&cfg.ForGracePeriod, "metrics.rules.alert.for-grace-period", defaultForGracePeriod, "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.")
	fs.DurationVar(&cfg.ResendDelay, "metrics.rules.alert.resend-delay", defaultResendDelay, "Minimum amount of time to wait before resending an alert to Alertmanager.")
	fs.StringVar(&cfg.PrometheusConfigAddress, "metrics.rules.prometheus-config", "", "Address of the Prometheus configuration. This is used to extract the"+
		" alertmanager configuration and the addresses of rules files."+
		"Note: If this is empty or `rule_files` is empty, Promscale rule-manager will not start. If `alertmanager_config` is empty, alerting will not be initialized.")
	return cfg
}

func Validate(cfg *Config) error {
	if cfg.PrometheusConfigAddress == "" {
		return nil
	}
	promCfg, err := prometheus_config.LoadFile(cfg.PrometheusConfigAddress, false, true, log.GetLogger())
	if err != nil {
		return fmt.Errorf("error loading Prometheus configuration file: %w", err)
	}
	cfg.PrometheusConfig = promCfg
	return nil
}
