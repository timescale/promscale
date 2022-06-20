// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rules

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgclient"
)

func TestRegexInConfig(t *testing.T) {
	cfg := DefaultConfig
	cfg.PrometheusConfigAddress = "./testdata/rules.glob.config.yaml"

	m, reloader, err := NewManager(context.Background(), prometheus.NewRegistry(), &pgclient.Client{}, &cfg)
	require.NoError(t, err)
	require.NoError(t, reloader())

	ruleGroups := m.RuleGroups()
	require.Equal(t, "g-one", ruleGroups[0].Name())
	require.Equal(t, "g-two", ruleGroups[1].Name())
}
