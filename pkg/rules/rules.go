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
	"github.com/prometheus/prometheus/storage"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/query"
)

const (
	DefaultQueueCapacity   = 10000
	DefaultOutageTolerance = time.Hour
	DefaultForGracePeriod  = time.Minute * 10
	DefaultResendDelay     = time.Minute
)

type Options struct {
	QueueCapacity   int
	OutageTolerance time.Duration
	ForGracePeriod  time.Duration
	ResendDelay     time.Duration
}

type manager struct {
	rulesManager     *prom_rules.Manager
	notifierManager  *notifier.Manager
	discoveryManager *discovery.Manager
}

func NewManager(ctx context.Context, r prometheus.Registerer, client *pgclient.Client, opts *Options) (*manager, error) {
	appendable := storage.Appendable(nil)
	queryable := storage.Queryable(nil)

	promqlEngine, err := query.NewEngineWithDefaults(log.GetLogger())
	if err != nil {
		return nil, fmt.Errorf("error creating PromQL engine with defaults: %w", err)
	}

	discoveryManagerNotify := discovery.NewManager(ctx, log.GetLogger(), discovery.Name("notify"))

	notifierManager := notifier.NewManager(&notifier.Options{
		QueueCapacity: opts.QueueCapacity,
		Registerer:    r,
		Do:            do,
	}, log.GetLogger())

	// For the moment, we do not have any external UI url, hence we provide an empty one.
	parsedUrl, err := url.Parse("")
	if err != nil {
		return nil, fmt.Errorf("parsing UI-URL: %w", err)
	}

	rulesManager := prom_rules.NewManager(&prom_rules.ManagerOptions{
		Appendable:      appendable,
		Context:         ctx,
		ExternalURL:     parsedUrl,
		Logger:          log.GetLogger(),
		NotifyFunc:      sendAlerts(notifierManager, parsedUrl.String()),
		Queryable:       queryable,
		QueryFunc:       engineQueryFunc(promqlEngine, client.Queryable()),
		Registerer:      r,
		OutageTolerance: opts.OutageTolerance,
		ForGracePeriod:  opts.ForGracePeriod,
		ResendDelay:     opts.ResendDelay,
	})
	return &manager{
		rulesManager:     rulesManager,
		notifierManager:  notifierManager,
		discoveryManager: discoveryManagerNotify,
	}, nil
}

func (m *manager) ApplyConfig(cfg *prometheus_config.Config) error {
	if err := m.applyDiscoveryManagerConfig(cfg); err != nil {
		return err
	}
	if err := m.applyNotifierManagerConfig(cfg); err != nil {
		return err
	}
	return nil
}

func (m *manager) applyDiscoveryManagerConfig(cfg *prometheus_config.Config) error {
	c := make(map[string]discovery.Configs)
	for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
		c[k] = v.ServiceDiscoveryConfigs
	}
	return errors.WithMessage(m.discoveryManager.ApplyConfig(c), "error applying config to discover manager")
}

func (m *manager) applyNotifierManagerConfig(cfg *prometheus_config.Config) error {
	return errors.WithMessage(m.notifierManager.ApplyConfig(cfg), "error applying config to notifier manager")
}

func (m *manager) Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string) error {
	return errors.WithMessage(m.rulesManager.Update(interval, files, externalLabels, externalURL), "error updating the rules manager")
}

// Run runs the managers and blocks on either a graceful exit or on error.
func (m *manager) Run() error {
	var g run.Group

	g.Add(func() error {
		log.Info("msg", "starting discovery manager...")
		return errors.WithMessage(m.discoveryManager.Run(), "error running discovery manager")
	}, func(err error) {})

	g.Add(func() error {
		log.Info("msg", "starting notifier manager...")
		m.notifierManager.Run(m.discoveryManager.SyncCh())
		return nil
	}, func(error) {
		log.Info("msg", "stopping notifier manager")
		m.notifierManager.Stop()
	})

	g.Add(func() error {
		log.Info("msg", "starting rules manager...")
		m.rulesManager.Run()
		return nil
	}, func(error) {
		log.Info("msg", "stopping rules manager")
		m.rulesManager.Stop()
	})

	return errors.WithMessage(g.Run(), "error running the rule manager groups")
}
