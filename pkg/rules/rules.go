package rules

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	prom_rules "github.com/prometheus/prometheus/rules"
	"github.com/timescale/promscale/pkg/pgclient"
	"net/url"
)

type manager struct {
	ruleManager prom_rules.Manager
}

func NewManager(ctx context.Context, r prometheus.Registerer, amURL *url.URL, client *pgclient.Client) *manager {

	rulesManager := prom_rules.NewManager(&prom_rules.ManagerOptions{})
}
