package runner

import (
	"flag"
	"fmt"
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
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
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
				c.ReadOnly = true
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
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
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
			metrics := api.InitMetrics()
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
