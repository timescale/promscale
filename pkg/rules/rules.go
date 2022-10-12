// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rules

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	"github.com/oklog/run"
	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_config "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"
	prom_rules "github.com/prometheus/prometheus/rules"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/rules/adapters"
	"github.com/timescale/promscale/pkg/telemetry"
)

type Manager struct {
	ctx                 context.Context
	rulesManager        *prom_rules.Manager
	notifierManager     *notifier.Manager
	discoveryManager    *discovery.Manager
	postRulesProcessing prom_rules.RuleGroupPostProcessFunc
}

func NewManager(ctx context.Context, r prometheus.Registerer, client *pgclient.Client, cfg *Config) (*Manager, func() error, error) {
	discoveryManagerNotify := discovery.NewManager(ctx, log.GetLogger(), discovery.Name("notify"))

	notifierManager := notifier.NewManager(&notifier.Options{
		QueueCapacity: cfg.NotificationQueueCapacity,
		Registerer:    r,
		Do:            do,
	}, log.GetLogger())

	// For the moment, we do not have any external UI url, hence we provide an empty one.
	parsedUrl, err := url.Parse("")
	if err != nil {
		return nil, nil, fmt.Errorf("parsing UI-URL: %w", err)
	}

	rulesManager := prom_rules.NewManager(&prom_rules.ManagerOptions{
		Appendable:      adapters.NewIngestAdapter(client.Inserter()),
		Queryable:       adapters.NewQueryAdapter(client.Queryable()),
		Context:         ctx,
		ExternalURL:     parsedUrl,
		Logger:          log.GetLogger(),
		NotifyFunc:      sendAlerts(notifierManager, parsedUrl.String()),
		QueryFunc:       engineQueryFunc(client.QueryEngine(), client.Queryable()),
		Registerer:      r,
		OutageTolerance: cfg.OutageTolerance,
		ForGracePeriod:  cfg.ForGracePeriod,
		ResendDelay:     cfg.ResendDelay,
	})

	manager := &Manager{
		ctx:              ctx,
		rulesManager:     rulesManager,
		notifierManager:  notifierManager,
		discoveryManager: discoveryManagerNotify,
	}
	return manager, manager.getReloader(cfg), nil
}

func InitTelemetry() {
	telemetry.Registry.Update("rules_enabled", "0")
	telemetry.Registry.Update("alerting_enabled", "0")
}

func (m *Manager) getReloader(cfg *Config) func() error {
	return func() error {
		err := Validate(cfg) // This refreshes the RulesCfg.PrometheusConfig entry in RulesCfg after reading the PrometheusConfigAddress.
		if err != nil {
			return fmt.Errorf("error validating rules-config: %w", err)
		}
		if err = m.ApplyConfig(cfg.PrometheusConfig); err != nil {
			return fmt.Errorf("error applying config: %w", err)
		}
		m.updateTelemetry(cfg)
		return nil
	}
}

func (m *Manager) updateTelemetry(cfg *Config) {
	if cfg.ContainsRules() {
		telemetry.Registry.Update("rules_enabled", "1")
		if cfg.ContainsAlertingConfig() {
			telemetry.Registry.Update("alerting_enabled", "1")
		} else {
			log.Debug("msg", "Alerting configuration not present in the given Prometheus configuration file. Alerting will not be initialized")
			telemetry.Registry.Update("alerting_enabled", "0")
		}
		return
	}
	log.Debug("msg", "Rules files not found. Rules and alerting configuration will not be initialized")
	telemetry.Registry.Update("rules_enabled", "0")
}

func (m *Manager) WithPostRulesProcess(f prom_rules.RuleGroupPostProcessFunc) {
	m.postRulesProcessing = f
}

func (m *Manager) ApplyConfig(cfg *prometheus_config.Config) error {
	if err := m.applyDiscoveryManagerConfig(cfg); err != nil {
		return err
	}
	if err := m.applyNotifierManagerConfig(cfg); err != nil {
		return err
	}

	// Get all rule files matching the configuration paths.
	var files []string
	for _, pat := range cfg.RuleFiles {
		fs, err := filepath.Glob(pat)
		if err != nil {
			return fmt.Errorf("error retrieving rule files for %s: %w", pat, err)
		}
		files = append(files, fs...)
	}
	if err := m.rulesManager.Update(time.Duration(cfg.GlobalConfig.EvaluationInterval), files, cfg.GlobalConfig.ExternalLabels, "", m.postRulesProcessing); err != nil {
		return fmt.Errorf("error updating rule-manager: %w", err)
	}
	return nil
}

func (m *Manager) applyDiscoveryManagerConfig(cfg *prometheus_config.Config) error {
	c := make(map[string]discovery.Configs)
	for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
		c[k] = v.ServiceDiscoveryConfigs
	}
	return errors.WithMessage(m.discoveryManager.ApplyConfig(c), "error applying config to discover manager")
}

func (m *Manager) applyNotifierManagerConfig(cfg *prometheus_config.Config) error {
	return errors.WithMessage(m.notifierManager.ApplyConfig(cfg), "error applying config to notifier manager")
}

func (m *Manager) RuleGroups() []*prom_rules.Group {
	return m.rulesManager.RuleGroups()
}

func (m *Manager) AlertingRules() []*prom_rules.AlertingRule {
	return m.rulesManager.AlertingRules()
}

// Run runs the managers and blocks on either a graceful exit or on error.
func (m *Manager) Run() error {
	var g run.Group

	g.Add(func() error {
		log.Debug("msg", "Starting discovery manager...")
		return errors.WithMessage(m.discoveryManager.Run(), "error running discovery manager")
	}, func(err error) {
		log.Debug("msg", "Stopping discovery manager")
	})

	g.Add(func() error {
		log.Debug("msg", "Starting notifier manager...")
		m.notifierManager.Run(m.discoveryManager.SyncCh())
		return nil
	}, func(error) {
		log.Debug("msg", "Stopping notifier manager")
		m.notifierManager.Stop()
	})

	g.Add(func() error {
		log.Debug("msg", "Starting internal rule-manager...")
		m.rulesManager.Run()
		return nil
	}, func(error) {
		log.Debug("msg", "Stopping internal rule-manager")
		m.rulesManager.Stop()
	})

	g.Add(func() error {
		// This stops all actors in the group on context done.
		<-m.ctx.Done()
		return nil
	}, func(err error) {})

	return errors.WithMessage(g.Run(), "error running the rule manager groups")
}
