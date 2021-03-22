// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/peterbourgon/ff/v3"
	"github.com/peterbourgon/ff/v3/ffyaml"
	"github.com/timescale/promscale/pkg/api"
	"github.com/timescale/promscale/pkg/limits"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/util"
)

type Config struct {
	ListenAddr                  string
	PgmodelCfg                  pgclient.Config
	LogCfg                      log.Config
	APICfg                      api.Config
	LimitsCfg                   limits.Config
	ConfigFile                  string
	TLSCertFile                 string
	TLSKeyFile                  string
	HaGroupLockID               int64
	PrometheusTimeout           time.Duration
	ElectionInterval            time.Duration
	Migrate                     bool
	StopAfterMigrate            bool
	UseVersionLease             bool
	InstallExtensions           bool
	UpgradeExtensions           bool
	UpgradePrereleaseExtensions bool
}

func ParseFlags(cfg *Config, args []string) (*Config, error) {
	var (
		fs             = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		corsOriginFlag string
		migrateOption  string
	)

	pgclient.ParseFlags(fs, &cfg.PgmodelCfg)
	log.ParseFlags(fs, &cfg.LogCfg)
	api.ParseFlags(fs, &cfg.APICfg)
	limits.ParseFlags(fs, &cfg.LimitsCfg)

	fs.StringVar(&cfg.ConfigFile, "config", "config.yml", "YAML configuration file path for Promscale.")
	fs.StringVar(&cfg.ListenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	fs.StringVar(&corsOriginFlag, "web-cors-origin", ".*", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`)
	fs.Int64Var(&cfg.HaGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Leader-election based high-availability. It is based on PostgreSQL advisory lock and requires a unique advisory lock ID per high-availability group. Only a single connector in each high-availability group will write data at one time. A value of 0 disables leader election.")
	fs.DurationVar(&cfg.PrometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Prometheus timeout duration for leader-election high-availability. The connector will resign if the associated Prometheus instance does not respond within the given timeout. This value should be a low multiple of the Prometheus scrape interval, big enough to prevent random flips.")
	fs.DurationVar(&cfg.ElectionInterval, "leader-election-scheduled-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")
	fs.StringVar(&migrateOption, "migrate", "true", "Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only].")
	fs.BoolVar(&cfg.UseVersionLease, "use-schema-version-lease", true, "Use schema version lease to prevent race conditions during migration.")
	fs.BoolVar(&cfg.InstallExtensions, "install-extensions", true, "Install TimescaleDB, Promscale extension.")
	fs.BoolVar(&cfg.UpgradeExtensions, "upgrade-extensions", true, "Upgrades TimescaleDB, Promscale extensions.")
	fs.BoolVar(&cfg.UpgradePrereleaseExtensions, "upgrade-prerelease-extensions", false, "Upgrades to pre-release TimescaleDB, Promscale extensions.")
	fs.StringVar(&cfg.TLSCertFile, "tls-cert-file", "", "TLS Certificate file for web server, leave blank to disable TLS.")
	fs.StringVar(&cfg.TLSKeyFile, "tls-key-file", "", "TLS Key file for web server, leave blank to disable TLS.")

	if err := util.ParseEnv("PROMSCALE", fs); err != nil {
		return nil, fmt.Errorf("error parsing env variables: %w", err)
	}
	// Deprecated: TS_PROM is the old prefix which is deprecated and in here
	// for legacy compatibility. Will be removed in the future. PROMSCALE prefix
	// takes precedence and will be used if the same variable with both prefixes
	// exist.
	if err := util.ParseEnv("TS_PROM", fs); err != nil {
		return nil, fmt.Errorf("error parsing env variables: %w", err)
	}

	if err := ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ffyaml.Parser),
		ff.WithAllowMissingConfigFile(true),
	); err != nil {
		return nil, fmt.Errorf("configuration error: %w", err)
	}

	// Checking if TLS files are not both set or both empty.
	if (cfg.TLSCertFile != "") != (cfg.TLSKeyFile != "") {
		return nil, fmt.Errorf("both TLS Ceriticate File and TLS Key File need to be provided for a valid TLS configuration")
	}

	corsOriginRegex, err := compileAnchoredRegexString(corsOriginFlag)
	if err != nil {
		return nil, fmt.Errorf("could not compile CORS regex string %v: %w", corsOriginFlag, err)
	}
	cfg.APICfg.AllowedOrigin = corsOriginRegex

	if err := api.Validate(&cfg.APICfg); err != nil {
		return nil, fmt.Errorf("error validating API configuration: %w", err)
	}
	if err := limits.Validate(&cfg.LimitsCfg); err != nil {
		return nil, fmt.Errorf("error validating limits configuration: %w", err)
	}

	cfg.StopAfterMigrate = false
	if strings.EqualFold(migrateOption, "true") {
		cfg.Migrate = true
	} else if strings.EqualFold(migrateOption, "false") {
		cfg.Migrate = false
	} else if strings.EqualFold(migrateOption, "only") {
		cfg.Migrate = true
		cfg.StopAfterMigrate = true
	} else {
		return nil, fmt.Errorf("Invalid option for migrate: %v. Valid options are [true, false, only]", migrateOption)
	}

	if cfg.APICfg.ReadOnly {
		flagset := make(map[string]bool)
		fs.Visit(func(f *flag.Flag) { flagset[f.Name] = true })
		if (flagset["migrate"] && cfg.Migrate) || (flagset["use-schema-version-lease"] && cfg.UseVersionLease) {
			return nil, fmt.Errorf("Migration flags not supported in read-only mode")
		}
		if flagset["leader-election-pg-advisory-lock-id"] && cfg.HaGroupLockID != 0 {
			return nil, fmt.Errorf("Invalid option for HA group lock ID, cannot enable HA mode and read-only mode")
		}
		if flagset["install-extensions"] && cfg.InstallExtensions {
			return nil, fmt.Errorf("Cannot install or update TimescaleDB extension in read-only mode")
		}
		cfg.Migrate = false
		cfg.StopAfterMigrate = false
		cfg.UseVersionLease = false
		cfg.InstallExtensions = false
		cfg.UpgradeExtensions = false
	}

	cfg.PgmodelCfg.UsesHA = cfg.HaGroupLockID != 0
	return cfg, nil
}
