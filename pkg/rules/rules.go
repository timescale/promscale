package rules

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_config "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/notifier"
	prom_rules "github.com/prometheus/prometheus/rules"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/rules/adapters"
)

type manager struct {
	rulesManager           *prom_rules.Manager
	notifierManager        *notifier.Manager
	discoveryManagerNotify *discovery.Manager
}

func NewManager(ctx context.Context, r prometheus.Registerer, uiURL string, client *pgclient.Client) (*manager, error) {
	appendable := adapters.NewIngestAdapter(client.Ingestor())
	queryable := adapters.NewQueryAdapter(client.Queryable())

	promqlEngine, err := query.NewEngineWithDefaults(log.GetLogger())
	if err != nil {
		return nil, fmt.Errorf("error creating PromQL engine with defaults: %w", err)
	}

	discoveryManagerNotify := discovery.NewManager(ctx, log.GetLogger(), discovery.Name("notify"))

	notifierManager := notifier.NewManager(&notifier.Options{
		QueueCapacity: 10,
		Registerer:    r,
		Do:            do,
	}, log.GetLogger())

	parsedUrl, err := url.Parse(uiURL)
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
		OutageTolerance: time.Hour,
		ForGracePeriod:  time.Minute * 10,
		ResendDelay:     time.Second * 10,
	})
	return &manager{
		rulesManager:           rulesManager,
		notifierManager:        notifierManager,
		discoveryManagerNotify: discoveryManagerNotify,
	}, nil
}

func (m *manager) ApplyConfig(cfg *prometheus_config.Config) error {
	c := make(map[string]discovery.Configs)
	for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
		c[k] = v.ServiceDiscoveryConfigs
	}
	err := m.discoveryManagerNotify.ApplyConfig(c)
	if err != nil {
		return fmt.Errorf("error applying discovery manager config: %w", err)
	}

	err = m.notifierManager.ApplyConfig(cfg)
	if err != nil {
		return fmt.Errorf("applying config to notifier-manager: %w", err)
	}
	return nil
}

// Update helps to hot-reload the rules manager.
func (m *manager) Update(interval time.Duration, files []string, externalLabels labels.Labels, externalURL string) error {
	return m.rulesManager.Update(interval, files, externalLabels, externalURL)
}

func (m *manager) Run() {
	go func() {
		if err := m.discoveryManagerNotify.Run(); err != nil {
			panic(err)
		}
	}()
	go func() {
		m.notifierManager.Run(m.discoveryManagerNotify.SyncCh())
	}()
	m.rulesManager.Run()
}

func (m *manager) Stop() {
	m.rulesManager.Stop()
}
