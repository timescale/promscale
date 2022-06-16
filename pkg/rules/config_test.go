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
		name           string
		config         Config
		expectedConfig Config
		containsRules  bool
		shouldError    bool
	}{
		{
			name:   "no prometheus config",
			config: Config{},
			expectedConfig: Config{
				PrometheusConfig: &prometheus_config.DefaultConfig,
			},
		},
		{
			name: "healthy config with no rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/no_rules.good.config.yaml",
			},
			expectedConfig: Config{
				PrometheusConfigAddress: "./testdata/no_rules.good.config.yaml",
				PrometheusConfig:        &prometheus_config.DefaultConfig,
			},
		},
		{
			name: "healthy config with rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/rules.good.config.yaml",
			},
			expectedConfig: Config{
				PrometheusConfigAddress: "./testdata/rules.good.config.yaml",
				PrometheusConfig:        prometheusConfigWithRules("testdata/rules.yaml"),
			},
			containsRules: true,
		},
		{
			name: "bad config",
			config: Config{
				PrometheusConfigAddress: "./testdata/no_rules.bad.config.yaml",
			},
			expectedConfig: Config{
				PrometheusConfigAddress: "testdata/no_rules.bad.config.yaml",
			},
			shouldError: true,
		},
		{
			name: "config with non existent rules",
			config: Config{
				PrometheusConfigAddress: "./testdata/non_existent_rules.good.config.yaml",
			},
			expectedConfig: Config{
				PrometheusConfigAddress: "./testdata/non_existent_rules.good.config.yaml", // This won't give an error since rules file addresses are validated when they are applied to rules-manager, not in Prometheus config loading.
				PrometheusConfig:        prometheusConfigWithRules("testdata/non_existent_rules.yaml"),
			},
			containsRules: true,
		},
		{
			name: "good with alerting config",
			config: Config{
				PrometheusConfigAddress: "./testdata/alert_config.good.config.yaml",
			},
			expectedConfig: Config{
				PrometheusConfigAddress: "./testdata/alert_config.good.config.yaml",
				PrometheusConfig: &prometheus_config.Config{
					GlobalConfig: prometheus_config.DefaultGlobalConfig,
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
								HTTPClientConfig: prometheus_common_config.HTTPClientConfig{
									FollowRedirects: true,
									EnableHTTP2:     true,
								},
								Scheme:     "https",
								APIVersion: "v2",
								Timeout:    model.Duration(10000000000),
							},
						},
					},
					RuleFiles: []string{"testdata/rules.yaml"},
				},
			},
			containsRules: true,
		},
	}
	for _, c := range cases {
		err := Validate(&c.config)
		if c.shouldError {
			require.NotNil(t, err, c.name)
		} else {
			require.Nil(t, err, c.name)
		}

		require.Equal(t, c.containsRules, c.config.ContainsRules(), c.name)

		if c.config.PrometheusConfig != nil {
			require.Equal(t, c.expectedConfig, c.config, c.name)
		}
	}
}

func prometheusConfigWithRules(files ...string) *prometheus_config.Config {
	c := prometheus_config.DefaultConfig
	c.RuleFiles = files
	return &c
}
