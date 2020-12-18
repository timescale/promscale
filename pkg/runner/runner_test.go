// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package runner

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/log"
)

func TestMain(m *testing.M) {
	flag.Parse()
	err := log.Init(log.Config{
		Level: "debug",
	})

	if err != nil {
		fmt.Println("Error initializing logger", err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func TestParseFlags(t *testing.T) {
	defaultConfig, err := ParseFlags(&Config{}, []string{})

	if err != nil {
		t.Fatal("error occured on default config with no arguments")
	}

	testCases := []struct {
		name        string
		args        []string
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
			args:        []string{"-config", "runner_test.go"},
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
				c.InstallTimescaleDB = false
				return c
			},
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
				"-install-timescaledb",
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
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
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
			name: "Env variable only, TS_PROM prefix",
			env: map[string]string{
				"TS_PROM_WEB_LISTEN_ADDRESS": "localhost:9201",
			},
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
			// In this case, we expect that PROMSCALE prefix gets precedence.
			name: "Env variable only, both prefixes",
			env: map[string]string{
				"PROMSCALE_WEB_LISTEN_ADDRESS": "localhost:9201",
				"TS_PROM_WEB_LISTEN_ADDRESS":   "127.0.0.1:9201",
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

func TestInitElector(t *testing.T) {
	// TODO: refactor the function to be fully testable without using a DB.
	testCases := []struct {
		name         string
		cfg          *Config
		shouldError  bool
		electionType reflect.Type
	}{
		{
			name: "Cannot create scheduled elector, no group lock ID and not rest election",
			cfg: &Config{
				HaGroupLockID: 0,
			},
		},
		{
			name: "Prometheus timeout not set for PG advisory lock",
			cfg: &Config{
				HaGroupLockID:     1,
				PrometheusTimeout: -1,
			},
			shouldError: true,
		},
		{
			name: "Can't get advisory lock, couldn't connect to DB",
			cfg: &Config{
				HaGroupLockID:     1,
				PrometheusTimeout: 0,
			},
			shouldError: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			metrics := api.InitMetrics(0)
			elector, err := initElector(c.cfg, metrics)

			switch {
			case err != nil && !c.shouldError:
				t.Errorf("Unexpected error, got %s", err.Error())
			case err == nil && c.shouldError:
				t.Errorf("Expected error, got nil")
			}

			if c.electionType != nil {
				if elector == nil {
					t.Fatalf("Expected to create elector, got nil")
				}

				v := reflect.ValueOf(elector).Elem().Field(0).Elem()

				if v.Type() != c.electionType {
					t.Errorf("Wrong type of elector created: got %v wanted %v", v.Type(), c.electionType)
				}
			}
		})
	}
}
