// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rules

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/oklog/run"
	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_config "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	prom_rules "github.com/prometheus/prometheus/rules"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/rules/adapters"
)

type Manager struct {
	rulesManager     *prom_rules.Manager
	notifierManager  *notifier.Manager
	discoveryManager *discovery.Manager
	stop             chan struct{}
}

func NewManager(ctx context.Context, r prometheus.Registerer, client *pgclient.Client, cfg *Config) (*Manager, error) {
	discoveryManagerNotify := discovery.NewManager(ctx, log.GetLogger(), discovery.Name("notify"))

	notifierManager := notifier.NewManager(&notifier.Options{
		QueueCapacity: cfg.NotificationQueueCapacity,
		Registerer:    r,
		Do:            do,
	}, log.GetLogger())

	// For the moment, we do not have any external UI url, hence we provide an empty one.
	parsedUrl, err := url.Parse("")
	if err != nil {
		return nil, fmt.Errorf("parsing UI-URL: %w", err)
	}

	rulesManager := prom_rules.NewManager(&prom_rules.ManagerOptions{
		Appendable:      adapters.NewIngestAdapter(client.Ingestor()),
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
	return &Manager{
		rulesManager:     rulesManager,
		notifierManager:  notifierManager,
		discoveryManager: discoveryManagerNotify,
		stop:             make(chan struct{}),
	}, nil
}

func (m *Manager) ApplyConfig(cfg *prometheus_config.Config) error {
	if err := m.applyDiscoveryManagerConfig(cfg); err != nil {
		return err
	}
	if err := m.applyNotifierManagerConfig(cfg); err != nil {
		return err
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

func (m *Manager) Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string) error {
	return errors.WithMessage(m.rulesManager.Update(interval, files, externalLabels, externalURL), "error updating the rules manager")
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
		// This stops all actors in the group on receiving request from manager.Stop()
		<-m.stop
		return nil
	}, func(err error) {})

	return errors.WithMessage(g.Run(), "error running the rule manager groups")
}

func (m *Manager) Stop() {
	close(m.stop)
}
