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
	DefaultQueueCapacity   = 10000
	DefaultOutageTolerance = time.Hour
	DefaultForGracePeriod  = time.Minute * 10
	DefaultResendDelay     = time.Minute
)

type Config struct {
	QueueCapacity           int
	OutageTolerance         time.Duration
	ForGracePeriod          time.Duration
	ResendDelay             time.Duration
	PrometheusConfigAddress string
	PrometheusConfig        *prometheus_config.Config
	Opts                    Options
}

type Options struct {
	ContainsAlertingConfig bool
	AlertingConfig         prometheus_config.AlertingConfig
	UseRulesManager        bool
	RulesFiles             []string
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.IntVar(&cfg.QueueCapacity, "metrics.rules.notification-queue-capacity", DefaultQueueCapacity, "The capacity of the queue for pending Alertmanager notifications.")
	fs.DurationVar(&cfg.OutageTolerance, "metrics.rules.outage-tolerance", DefaultOutageTolerance, "Max time to tolerate Promscale outage for restoring \"for\" state of alert.")
	fs.DurationVar(&cfg.ForGracePeriod, "metrics.rules.grace-period", DefaultForGracePeriod, "Minimum duration between alert and restored \"for\" state. This is maintained only for alerts with configured \"for\" time greater than grace period.")
	fs.DurationVar(&cfg.ResendDelay, "metrics.rules.resend-delay", DefaultResendDelay, "Minimum amount of time to wait before resending an alert to Alertmanager.")
	fs.StringVar(&cfg.PrometheusConfigAddress, "metrics.rules.prometheus-config", "", "Address of the Prometheus configuration. This is used to extract the"+
		" alertmanager configuration and the addresses of rules files."+
		"Note: If this is empty or `rule_files` is empty, Promscale rule-manager will not start. If `alertmanager_config` is empty, alerting will not be initialized.")
	return cfg
}

// Validate validates the configuration file and runs validation against (if) provided
// Prometheus configuration file.
func Validate(cfg *Config) error {
	if cfg.PrometheusConfigAddress == "" {
		return nil
	}
	promCfg, err := prometheus_config.LoadFile(cfg.PrometheusConfigAddress, false, true, log.GetLogger())
	if err != nil {
		return fmt.Errorf("error loading Prometheus configuration file: %w", err)
	}
	cfg.PrometheusConfig = promCfg

	if len(promCfg.RuleFiles) > 0 {
		cfg.Opts.UseRulesManager = true
		cfg.Opts.RulesFiles = promCfg.RuleFiles

		// Only check alerting config if rules files are available. Otherwise, there is no use for
		// checking alerting config.
		if promCfg.AlertingConfig.AlertRelabelConfigs == nil && promCfg.AlertingConfig.AlertmanagerConfigs == nil {
			// todo: test this and see if it works
			log.Info("msg", "Alerting configuration not present in the given Prometheus configuration file. Alerting will not be initialized")
		} else {
			cfg.Opts.ContainsAlertingConfig = true
			cfg.Opts.AlertingConfig = promCfg.AlertingConfig
		}
	} else {
		log.Info("msg", "Rules files not found in the given Prometheus configuration file. Both rule-manager and alerting will not be initialized")
	}
	return nil
}
