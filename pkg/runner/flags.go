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
	"github.com/timescale/promscale/pkg/tenancy"
	"github.com/timescale/promscale/pkg/tracer"
	"github.com/timescale/promscale/pkg/util"
)

type Config struct {
	ListenAddr                  string
	ThanosStoreAPIListenAddr    string
	OTLPGRPCListenAddr          string
	TraceQueryListenAddr        string
	PgmodelCfg                  pgclient.Config
	LogCfg                      log.Config
	TracerCfg                   tracer.Config
	APICfg                      api.Config
	LimitsCfg                   limits.Config
	TenancyCfg                  tenancy.Config
	ConfigFile                  string
	DatasetConfig               string
	TLSCertFile                 string
	TLSKeyFile                  string
	ThroughputInterval          time.Duration
	Migrate                     bool
	StopAfterMigrate            bool
	UseVersionLease             bool
	InstallExtensions           bool
	UpgradeExtensions           bool
	UpgradePrereleaseExtensions bool
	StartupOnly                 bool
}

var (
	aliasDescTemplate    = "Alias for -%s flag. "
	deprecatedFlagSuffix = "(DEPRECATED) Will be removed in version 0.11.0"
	flagAliases          = map[string][]string{
		"auth.tls-cert-file":                {"tls-cert-file"},
		"auth.tls-key-file":                 {"tls-key-file"},
		"cache.memory-target":               {"memory-target"},
		"db.app":                            {"app"},
		"db.connection-timeout":             {"db-connection-timeout"},
		"db.connections-max":                {"db-connections-max"},
		"db.host":                           {"db-host"},
		"db.name":                           {"db-name"},
		"db.num-writer-connections":         {"db-writer-connection-concurrency"},
		"db.password":                       {"db-password"},
		"db.port":                           {"db-port"},
		"db.read-only":                      {"read-only"},
		"db.ssl-mode":                       {"db-ssl-mode"},
		"db.statements-cache":               {"db-statements-cache"},
		"db.uri":                            {"db-uri"},
		"db.user":                           {"db-user"},
		"metrics.async-acks":                {"async-acks"},
		"metrics.cache.exemplar.size":       {"exemplar-cache-size"},
		"metrics.cache.labels.size":         {"labels-cache-size"},
		"metrics.cache.metrics.size":        {"metrics-cache-size"},
		"metrics.cache.series.initial-size": {"series-cache-initial-size"},
		"metrics.cache.series.max-bytes":    {"series-cache-max-bytes"},
		"metrics.high-availability":         {"high-availability"},
		"metrics.ignore-samples-written-to-compressed-chunks": {"ignore-samples-written-to-compressed-chunks"},
		"metrics.multi-tenancy":                               {"multi-tenancy"},
		"metrics.multi-tenancy.allow-non-tenants":             {"multi-tenancy-allow-non-tenants"},
		"metrics.multi-tenancy.valid-tenants":                 {"multi-tenancy-valid-tenants"},
		"metrics.promql.default-subquery-step-interval":       {"promql-default-subquery-step-interval"},
		"metrics.promql.lookback-delta":                       {"promql-lookback-delta"},
		"metrics.promql.max-points-per-ts":                    {"promql-max-points-per-ts"},
		"metrics.promql.max-samples":                          {"promql-max-samples"},
		"metrics.promql.query-timeout":                        {"promql-query-timeout"},
		"startup.install-extensions":                          {"install-extensions"},
		"startup.upgrade-extensions":                          {"upgrade-extensions"},
		"startup.upgrade-prerelease-extensions":               {"upgrade-prerelease-extensions"},
		"startup.use-schema-version-lease":                    {"use-schema-version-lease"},
		"telemetry.log.format":                                {"log-format"},
		"telemetry.log.level":                                 {"log-level"},
		"telemetry.log.throughput-report-interval":            {"tput-report"},
		"thanos.store-api.server-address":                     {"thanos-store-api-listen-address"},
		"tracing.otlp.server-address":                         {"otlp-grpc-server-listen-address"},
		"web.auth.bearer-token":                               {"bearer-token"},
		"web.auth.bearer-token-file":                          {"bearer-token-file"},
		"web.auth.password":                                   {"auth-password"},
		"web.auth.password-file":                              {"auth-password-file"},
		"web.auth.username":                                   {"auth-username"},
		"web.cors-origin":                                     {"web-cors-origin"},
		"web.enable-admin-api":                                {"web-enable-admin-api"},
		"web.listen-address":                                  {"web-listen-address"},
		"web.telemetry-path":                                  {"web-telemetry-path"},
	}
)

func ParseFlags(cfg *Config, args []string) (*Config, error) {
	var (
		fs = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)

		corsOriginFlag string
		migrateOption  string
		skipMigrate    bool
	)

	pgclient.ParseFlags(fs, &cfg.PgmodelCfg)
	log.ParseFlags(fs, &cfg.LogCfg)
	tracer.ParseFlags(fs, &cfg.TracerCfg)
	api.ParseFlags(fs, &cfg.APICfg)
	limits.ParseFlags(fs, &cfg.LimitsCfg)
	tenancy.ParseFlags(fs, &cfg.TenancyCfg)

	fs.StringVar(&cfg.ConfigFile, "config", "config.yml", "YAML configuration file path for Promscale.")
	fs.StringVar(&cfg.ListenAddr, "web.listen-address", ":9201", "Address to listen on for web endpoints.")
	fs.StringVar(&cfg.ThanosStoreAPIListenAddr, "thanos.store-api.server-address", "", "Address to listen on for Thanos Store API endpoints.")
	fs.StringVar(&cfg.OTLPGRPCListenAddr, "tracing.otlp.server-address", ":9202", "Address to listen on for OpenTelemetry OTLP GRPC server.")
	fs.StringVar(&cfg.TraceQueryListenAddr, "tracing.query.server-address", ":9203", "Address to listen for HTTP server for serving tracing queries. "+
		"This server responds in the lines of the Jaeger HTTP API.")
	fs.StringVar(&corsOriginFlag, "web.cors-origin", ".*", `Regex for CORS origin. It is fully anchored. Example: 'https?://(domain1|domain2)\.com'`)
	fs.DurationVar(&cfg.ThroughputInterval, "telemetry.log.throughput-report-interval", time.Second, "Duration interval at which throughput should be reported. Setting duration to `0` will disable reporting throughput, otherwise, an interval with unit must be provided, e.g. `10s` or `3m`.")
	fs.StringVar(&migrateOption, "migrate", "true", fmt.Sprintf("Update the Prometheus SQL schema to the latest version. Valid options are: [true, false, only]. %s", deprecatedFlagSuffix))
	fs.StringVar(&cfg.DatasetConfig, "startup.dataset.config", "", "Dataset configuration in YAML format for Promscale. It is used for setting various dataset configuration like default metric chunk interval")
	fs.BoolVar(&cfg.StartupOnly, "startup.only", false, "Only run startup configuration with Promscale (i.e. migrate) and exit. Can be used to run promscale as an init container for HA setups.")
	fs.BoolVar(&skipMigrate, "startup.skip-migrate", false, "Skip migrating Promscale SQL schema to latest version on startup.")

	fs.BoolVar(&cfg.UseVersionLease, "startup.use-schema-version-lease", true, "Use schema version lease to prevent race conditions during migration.")
	fs.BoolVar(&cfg.InstallExtensions, "startup.install-extensions", true, "Install TimescaleDB, Promscale extension.")
	fs.BoolVar(&cfg.UpgradeExtensions, "startup.upgrade-extensions", true, "Upgrades TimescaleDB, Promscale extensions.")
	fs.BoolVar(&cfg.UpgradePrereleaseExtensions, "startup.upgrade-prerelease-extensions", false, "Upgrades to pre-release TimescaleDB, Promscale extensions.")
	fs.StringVar(&cfg.TLSCertFile, "auth.tls-cert-file", "", "TLS Certificate file used for server authentication, leave blank to disable TLS. NOTE: this option is used for all servers that Promscale runs (web and GRPC).")
	fs.StringVar(&cfg.TLSKeyFile, "auth.tls-key-file", "", "TLS Key file for server authentication, leave blank to disable TLS. NOTE: this option is used for all servers that Promscale runs (web and GRPC).")

	addAliases(fs, flagAliases, deprecatedFlagSuffix)

	if err := util.ParseEnv("PROMSCALE", fs); err != nil {
		return nil, fmt.Errorf("error parsing env variables: %w", err)
	}

	if err := ff.Parse(fs, args,
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ffyaml.Parser),
		ff.WithAllowMissingConfigFile(true),
	); err != nil {
		return nil, fmt.Errorf("configuration error: %w", err)
	}

	// Warn about deprecated flags.
	if flagsForUpdate := deprecatedFlagsUsed(fs); len(flagsForUpdate) > 0 {
		log.Warn("msg", "Using deprecated flags that will be dropped in a future version. Please update to new flag names.")
		for oldFlag, newFlag := range flagsForUpdate {
			log.Warn("msg", fmt.Sprintf("Update deprecated flag %s to new flag %s", oldFlag, newFlag))
		}
	}
	// Migrate flag is a special case that needs to be logged separately.
	switch migrateOption {
	case "false":
		log.Warn("msg", "Using deprecated flag `migrate` set to `false` which will be dropped in a future version. "+
			"Please update you configuration by using `startup.skip-migrate` flag.")
	case "only":
		log.Warn("msg", "Using deprecated flag `migrate` set to `only` which will be dropped in a future version. "+
			"Please update you configuration by using `startup.only` flag.")
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

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
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

	if skipMigrate {
		cfg.Migrate = false
	}

	if cfg.APICfg.ReadOnly {
		flagset := make(map[string]bool)
		fs.Visit(func(f *flag.Flag) { flagset[f.Name] = true })
		if (flagset["migrate"] && cfg.Migrate) || (flagset["use-schema-version-lease"] && cfg.UseVersionLease) {
			return nil, fmt.Errorf("Migration flags not supported in read-only mode")
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

	if cfg.APICfg.HighAvailability {
		cfg.PgmodelCfg.UsesHA = true
	}
	return cfg, nil
}

func validate(cfg *Config) error {
	if err := api.Validate(&cfg.APICfg); err != nil {
		return fmt.Errorf("error validating API configuration: %w", err)
	}
	if err := limits.Validate(&cfg.LimitsCfg); err != nil {
		return fmt.Errorf("error validating limits configuration: %w", err)
	}
	if err := pgclient.Validate(&cfg.PgmodelCfg, cfg.LimitsCfg); err != nil {
		return fmt.Errorf("error validating client configuration: %w", err)
	}
	if err := tenancy.Validate(&cfg.TenancyCfg); err != nil {
		return fmt.Errorf("error validating multi-tenancy configuration: %w", err)
	}
	return nil
}

func addAliases(fs *flag.FlagSet, aliases map[string][]string, descSuffix string) {
	aliasDescFormat := aliasDescTemplate + descSuffix
	set := false
	fs.VisitAll(func(f *flag.Flag) {
		if flagAliases, ok := aliases[f.Name]; ok {
			set = false
			for _, alias := range flagAliases {
				set = true
				fs.Var(f.Value, alias, fmt.Sprintf(aliasDescFormat, f.Name))
			}

			if !set {
				panic(fmt.Sprintf("trying to set an flag alias for a flag that is missing: %s", f.Name))
			}
		}
	})
}

func deprecatedFlagsUsed(fs *flag.FlagSet) map[string]string {
	var (
		result      = make(map[string]string)
		newFlagName string
		count       int
	)

	fs.Visit(func(f *flag.Flag) {
		if strings.Contains(f.Usage, deprecatedFlagSuffix) {
			i, err := fmt.Sscanf(f.Usage, aliasDescTemplate, &newFlagName)

			switch {
			case f.Name == "migrate":
				// Migrate flag is a special case since it's not just a rename
				// of an old flag. We handle it separately when logging
				// use of deprecated flags.
				return
			case err != nil:
				fallthrough
			case i != 1:
				panic("deprecated flag usage not set in the correct format")
			}

			result[f.Name] = newFlagName
			count++
		}
	})

	return result
}
