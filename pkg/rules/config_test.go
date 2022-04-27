// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package rules

import (
	"testing"

	"github.com/stretchr/testify/require"

	prometheus_common_config "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	prometheus_config "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name            string
		config          Config
		expectedOptions Options
		shouldError     bool
	}{
		{
			name:   "no prometheus config",
			config: Config{},
		},
		{
			name: "healthy config with no rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/no_rules.good.config.yaml",
			},
		},
		{
			name: "healthy config with rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/rules.good.config.yaml",
			},
			expectedOptions: Options{
				UseRulesManager: true,
				RulesFiles:      []string{"testdata/rules.yaml"},
			},
		},
		{
			name: "bad config",
			config: Config{
				PrometheusConfigAddress: "./testdata/no_rules.bad.config.yaml",
			},
			shouldError: true,
		},
		{
			name: "bad config with non existent rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/non_existent_rules.good.config.yaml",
			},
			expectedOptions: Options{
				UseRulesManager: true,
				RulesFiles:      []string{"testdata/non_existent_rules.yaml"},
			},
			// No error since rules file addresses are validated when they are applied to rules-manager, not in Prometheus config loading.
		},
		{
			name: "good with alerting config",
			config: Config{
				PrometheusConfigAddress: "./testdata/alert_config.good.config.yaml",
			},
			expectedOptions: Options{
				ContainsAlertingConfig: true,
				AlertingConfig: prometheus_config.AlertingConfig{
					AlertRelabelConfigs: nil,
					AlertmanagerConfigs: prometheus_config.AlertmanagerConfigs{
						{
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{
									{
										Targets: []model.LabelSet{},
										Source:  "0",
									},
								},
							},
							HTTPClientConfig: prometheus_common_config.HTTPClientConfig{FollowRedirects: true},
							Scheme:           "https",
							APIVersion:       "v2",
							Timeout:          model.Duration(10000000000),
						},
					},
				},
				UseRulesManager: true,
				RulesFiles:      []string{"testdata/rules.yaml"},
			},
		},
	}
	for _, c := range cases {
		err := Validate(&c.config)
		if c.shouldError {
			require.NotNil(t, err, c.name)
		} else {
			require.Nil(t, err, c.name)
		}
		require.Equal(t, c.expectedOptions, c.config.Opts, c.name)
	}
}
