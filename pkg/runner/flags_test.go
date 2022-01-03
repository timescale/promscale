// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package runner

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestParseFlags(t *testing.T) {
	defaultConfig, err := ParseFlags(&Config{}, []string{})
	if err != nil {
		t.Fatal("error occured on default config with no arguments")
	}

	testCases := []struct {
		name        string
		args        []string
		env         map[string]string
		result      func(Config) Config
		shouldError bool
	}{
		{
			name:   "Default config",
			args:   []string{},
			result: func(c Config) Config { return c },
		},
		{
			name:        "Invalid flag error",
			args:        []string{"-foo", "bar"},
			shouldError: true,
		},
		{
			name:        "Invalid config file",
			args:        []string{"-config", "flags_test.go"},
			shouldError: true,
		},
		{
			name:        "CORS Origin regex error",
			args:        []string{"-web-cors-origin", "["},
			shouldError: true,
		},
		{
			name: "Don't migrate",
			args: []string{"-migrate", "false"},
			result: func(c Config) Config {
				c.Migrate = false
				return c
			},
		},
		{
			name:        "test removed promql flags",
			args:        []string{"-promql-enable-feature", "promql-at-modifier"},
			shouldError: true,
		},
		{
			name: "enable disabled-features",
			args: []string{"-enable-feature", "promql-at-modifier,tracing", "-tracing.otlp.server-address", ":8080"},
			result: func(c Config) Config {
				c.OTLPGRPCListenAddr = ":8080"
				c.APICfg.PromscaleEnabledFeatureList = []string{"promql-at-modifier", "tracing"}
				c.APICfg.EnabledFeatureMap = map[string]struct{}{
					"promql-at-modifier": {},
					"tracing":            {},
				}
				return c
			},
		},
		{
			name: "Only migrate",
			args: []string{"-migrate", "only"},
			result: func(c Config) Config {
				c.Migrate = true
				c.StopAfterMigrate = true
				return c
			},
		},
		{
			name:        "Invalid migrate option",
			args:        []string{"-migrate", "invalid"},
			shouldError: true,
		},
		{
			name: "Read-only mode",
			args: []string{"-read-only"},
			result: func(c Config) Config {
				c.APICfg.ReadOnly = true
				c.Migrate = false
				c.StopAfterMigrate = false
				c.UseVersionLease = false
				c.InstallExtensions = false
				c.UpgradeExtensions = false
				return c
			},
		},
		{
			name:        "Invalid migrate option",
			args:        []string{"-migrate", "invalid"},
			shouldError: true,
		},
		{
			name: "Running HA and read-only error",
			args: []string{
				"-leader-election-pg-advisory-lock-id", "1",
				"-read-only",
			},
			shouldError: true,
		},
		{
			name: "Running migrate and read-only error",
			args: []string{
				"-migrate", "true",
				"-read-only",
			},
			shouldError: true,
		},
		{
			name: "Running install TimescaleDB and read-only error",
			args: []string{
				"-install-extensions",
				"-read-only",
			},
			shouldError: true,
		},
		{
			name: "invalid TLS setup, missing key file",
			args: []string{
				"-tls-cert-file", "foo",
			},
			shouldError: true,
		},
		{
			name: "invalid TLS setup, missing cert file",
			args: []string{
				"-tls-key-file", "foo",
			},
			shouldError: true,
		},
		{
			name: "invalid auth setup",
			args: []string{
				"-auth-username", "foo",
			},
			shouldError: true,
		},
		{
			name: "invalid env variable type causing parse error",
			env: map[string]string{
				"PROMSCALE_INSTALL_EXTENSIONS": "foobar",
			},
			shouldError: true,
		},
		{
			name:        "setting tracing parameter without tracing enabled should fail",
			args:        []string{"-otlp-grpc-server-listen-address", "bar"},
			shouldError: true,
		},
		{
			name:        "enable feature should fail on unknown feature",
			args:        []string{"-enable-feature", "unknown"},
			shouldError: true,
		},
		{
			name:        "enable tracing should fail without OTLP",
			args:        []string{"-enable-feature", "tracing"},
			shouldError: true,
		},
		{
			name:        "OTLP should fail without enable tracing",
			args:        []string{"-otlp-grpc-server-listen-address", "someaddress"},
			shouldError: true,
		},
		{
			name: "enable feature should populate map of enabled features",
			args: []string{"-enable-feature", "tracing,promql-at-modifier,promql-negative-offset", "-otlp-grpc-server-listen-address", "someaddress"},
			result: func(c Config) Config {
				c.OTLPGRPCListenAddr = "someaddress"
				c.APICfg.EnabledFeatureMap = map[string]struct{}{
					"tracing":                {},
					"promql-at-modifier":     {},
					"promql-negative-offset": {},
				}
				c.APICfg.PromscaleEnabledFeatureList = []string{"tracing", "promql-at-modifier", "promql-negative-offset"}
				return c
			},
		},
		{
			name: "test deprecated CLI flag",
			args: []string{"-db-writer-connection-concurrency", "10"},
			result: func(c Config) Config {
				c.PgmodelCfg.WriteConnectionsPerProc = 10
				return c
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Clearing environment variables so they don't interfere with the test.
			os.Clearenv()
			for name, value := range c.env {
				if err := os.Setenv(name, value); err != nil {
					t.Fatalf("unexpected error when setting env variable: name %s, value %s, error %s", name, value, err)
				}
			}
			config, err := ParseFlags(&Config{}, c.args)
			if c.shouldError {
				if err == nil {
					t.Fatal("Unexpected error result, should not be nil")
				}
				return
			} else if err != nil {
				t.Fatalf("Unexpected returned error: %s", err.Error())
			}

			expected := c.result(*defaultConfig)
			if !reflect.DeepEqual(*config, expected) {
				t.Fatalf("Unexpected config returned\nwanted:\n%+v\ngot:\n%+v\n", expected, *config)
			}
		})
	}
}

func TestParseFlagsConfigPrecedence(t *testing.T) {
	// Clearing environment variables so they don't interfere with the test.
	os.Clearenv()
	defaultConfig, err := ParseFlags(&Config{}, []string{})

	if err != nil {
		t.Fatalf("error occured on default config with no arguments: %s", err)
	}

	testCases := []struct {
		name               string
		args               []string
		env                map[string]string
		configFileContents string
		result             func(Config) Config
	}{
		{
			name:   "Default config",
			result: func(c Config) Config { return c },
		},
		{
			name:               "Config file only",
			configFileContents: "web-listen-address: localhost:9201",
			result: func(c Config) Config {
				c.ListenAddr = "localhost:9201"
				return c
			},
		},
		{
			name: "Env variable only, PROMSCALE prefix",
			env: map[string]string{
				"PROMSCALE_WEB_LISTEN_ADDRESS": "localhost:9201",
			},
			result: func(c Config) Config {
				c.ListenAddr = "localhost:9201"
				return c
			},
		},
		{
			name: "Env variable takes precedence over config file setting",
			env: map[string]string{
				"PROMSCALE_WEB_LISTEN_ADDRESS": "localhost:9201",
			},
			configFileContents: "web-listen-address: 127.0.0.1:9201",
			result: func(c Config) Config {
				c.ListenAddr = "localhost:9201"
				return c
			},
		},
		{
			name: "CLI arg takes precedence over env variable",
			args: []string{
				"-web-listen-address", "localhost:9201",
			},
			env: map[string]string{
				"PROMSCALE_WEB_LISTEN_ADDRESS": "127.0.0.1:9201",
			},
			result: func(c Config) Config {
				c.ListenAddr = "localhost:9201"
				return c
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Clearing environment variables so they don't interfere with the test.
			os.Clearenv()
			for name, value := range c.env {
				if err := os.Setenv(name, value); err != nil {
					t.Fatalf("unexpected error when setting env variable: name %s, value %s, error %s", name, value, err)
				}
			}

			var configFilePath string
			if c.configFileContents != "" {
				f, err := ioutil.TempFile("", "promscale.yml")
				if err != nil {
					t.Fatalf("unexpected error when creating config file: %s", err)
				}

				configFilePath = f.Name()

				defer os.Remove(f.Name())

				if _, err := f.Write([]byte(c.configFileContents)); err != nil {
					t.Fatalf("unexpected error while writing configuration file: %s", err)
				}
				if err := f.Close(); err != nil {
					t.Fatalf("unexpected error while closing configuration file: %s", err)
				}

				// Add config file path to args.
				c.args = append(c.args, "-config="+f.Name())
			}

			config, err := ParseFlags(&Config{}, c.args)
			if err != nil {
				t.Fatalf("unexpected error, all test cases should pass without error: %s", err)
			}

			expected := c.result(*defaultConfig)

			// Need to account for change in config file path, which
			// would otherwise be set to default `config.yml`.
			if configFilePath != "" {
				expected.ConfigFile = configFilePath
			}

			if !reflect.DeepEqual(*config, expected) {
				t.Fatalf("Unexpected config returned\nwanted:\n%+v\ngot:\n%+v\n", expected, *config)
			}
		})
	}
}
