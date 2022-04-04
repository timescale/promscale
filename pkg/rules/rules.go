package rules

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/query"
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	prom_rules "github.com/prometheus/prometheus/rules"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/rules/adapters"
)

type manager struct {
	ruleManager prom_rules.Manager
}

func NewManager(ctx context.Context, r prometheus.Registerer, amURL *url.URL, client *pgclient.Client) (*manager, error) {
	appendable := adapters.NewIngestAdapter(client.Ingestor())
	queryable := adapters.NewQueryAdapter(client.Queryable())
	promqlEngine, err := query.NewEngineWithDefaults(log.GetLogger())
	if err != nil {
		return nil, fmt.Errorf("error creating PromQL engine with defaults: %w", err)
	}
	rulesManager := prom_rules.NewManager(&prom_rules.ManagerOptions{
		Appendable: appendable,
		Queryable:  queryable,
		QueryFunc:  engineQueryFunc(promqlEngine, client.Queryable()),
	})
}
