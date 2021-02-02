// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/version"
)

// Config for the database.
type Config struct {
	AppName                 string
	Host                    string
	Port                    int
	User                    string
	Password                string
	Database                string
	SslMode                 string
	DbConnectRetries        int
	DbConnectionTimeout     time.Duration
	AsyncAcks               bool
	ReportInterval          int
	LabelsCacheSize         uint64
	MetricsCacheSize        uint64
	SeriesCacheSize         uint64
	WriteConnectionsPerProc int
	MaxConnections          int
	UsesHA                  bool
	DbUri                   string
}

const (
	defaultDBUri          = ""
	defaultDBHost         = "localhost"
	defaultDBPort         = 5432
	defaultDBUser         = "postgres"
	defaultDBName         = "timescale"
	defaultDBPassword     = ""
	defaultSSLMode        = "require"
	defaultConnectionTime = time.Minute
)

var (
	DefaultApp         = fmt.Sprintf("promscale@%s", version.Version)
	excessDBFlagsError = fmt.Errorf("failed to build DB credentials with provided flags. Please use either db flags or db-uri not both")
)

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	fs.StringVar(&cfg.AppName, "app", DefaultApp, "'app' sets application_name in database connection string. This is helpful during debugging when looking at pg_stat_activity.")
	fs.StringVar(&cfg.Host, "db-host", defaultDBHost, "Host for TimescaleDB/Vanilla Postgres.")
	fs.IntVar(&cfg.Port, "db-port", defaultDBPort, "TimescaleDB/Vanilla Postgres connection password.")
	fs.StringVar(&cfg.User, "db-user", defaultDBUser, "TimescaleDB/Vanilla Postgres user.")
	fs.StringVar(&cfg.Password, "db-password", defaultDBPassword, "Password for connecting to TimescaleDB/Vanilla Postgres.")
	fs.StringVar(&cfg.Database, "db-name", defaultDBName, "Database name.")
	fs.StringVar(&cfg.SslMode, "db-ssl-mode", defaultSSLMode, "TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass 'allow' as value.")
	fs.IntVar(&cfg.DbConnectRetries, "db-connect-retries", 0, "Number of retries Promscale should make for establishing connection with the database.")
	fs.DurationVar(&cfg.DbConnectionTimeout, "db-connection-timeout", defaultConnectionTime, "Timeout for establishing the connection between Promscale and TimescaleDB.")
	fs.BoolVar(&cfg.AsyncAcks, "async-acks", false, "Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss.")
	fs.IntVar(&cfg.ReportInterval, "tput-report", 0, "Interval in seconds at which throughput should be reported.")
	fs.Uint64Var(&cfg.LabelsCacheSize, "labels-cache-size", 10000, "Maximum number of labels to cache.")
	fs.Uint64Var(&cfg.MetricsCacheSize, "metrics-cache-size", cache.DefaultMetricCacheSize, "Maximum number of metric names to cache.")
	fs.IntVar(&cfg.WriteConnectionsPerProc, "db-writer-connection-concurrency", 4, "Maximum number of database connections for writing per go process.")
	fs.IntVar(&cfg.MaxConnections, "db-connections-max", -1, "Maximum number of connections to the database that should be opened at once. It defaults to 80% of the maximum connections that the database can handle.")
	fs.StringVar(&cfg.DbUri, "db-uri", defaultDBUri, "TimescaleDB/Vanilla Postgres DB URI. Example DB URI `postgres://postgres:password@localhost:5432/timescale?sslmode=require`")
	return cfg
}

// GetConnectionStr returns a Postgres connection string
func (cfg *Config) GetConnectionStr() (string, error) {
	// if DBURI is default build the connStr with DB flags
	// else as DBURI isn't default check if db flags are default if we notice DBURI + DB flags not default give an error
	// Now as DBURI isn't default and DB flags are default build a connStr for DBURI.
	if cfg.DbUri == defaultDBUri {
		return fmt.Sprintf("application_name=%s host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=%d",
			cfg.AppName, cfg.Host, cfg.Port, cfg.User, cfg.Database, cfg.Password, cfg.SslMode, int(cfg.DbConnectionTimeout.Seconds())), nil
	} else if cfg.AppName != DefaultApp || cfg.Database != defaultDBName || cfg.Host != defaultDBHost || cfg.Port != defaultDBPort ||
		cfg.User != defaultDBUser || cfg.Password != defaultDBPassword || cfg.SslMode != defaultSSLMode ||
		cfg.DbConnectionTimeout != defaultConnectionTime {
		return "", excessDBFlagsError
	}

	return cfg.DbUri, nil
}

func (cfg *Config) GetNumConnections() (min int, max int, numCopiers int, err error) {
	maxProcs := runtime.GOMAXPROCS(-1)
	if cfg.WriteConnectionsPerProc < 1 {
		return 0, 0, 0, fmt.Errorf("invalid number of connections-per-proc %v, must be at least 1", cfg.WriteConnectionsPerProc)
	}
	perProc := cfg.WriteConnectionsPerProc
	max = cfg.MaxConnections
	if max < 1 {
		connStr, err := cfg.GetConnectionStr()
		if err != nil {
			return 0, 0, 0, err
		}
		conn, err := pgx.Connect(context.Background(), connStr)
		if err != nil {
			return 0, 0, 0, err
		}
		defer func() { _ = conn.Close(context.Background()) }()
		var maxStr string
		row := conn.QueryRow(context.Background(), "SHOW max_connections")
		err = row.Scan(&maxStr)
		if err != nil {
			return 0, 0, 0, err
		}
		max, err = strconv.Atoi(maxStr)
		if err != nil {
			log.Warn("err", err, "msg", "invalid value from postgres max_connections")
			max = 100
		}

		//In HA setups
		if cfg.UsesHA {
			max = max / 2
		}

		if max <= 1 {
			log.Warn("msg", "database can only handle 1 connection")
			return 1, 1, 1, nil
		}
		// we try to only use 80% the database connections, capped at 50
		max = int(0.8 * float32(max))
		if max > 50 {
			max = 50
		}
	}

	// we want to leave some connections for non-copier usages, so in the event
	// there aren't enough connections available to satisfy our per-process
	// preferences we'll scale down the number of copiers
	min = maxProcs
	if max <= min {
		log.Warn("msg", fmt.Sprintf("database can only handle %v connection; connector has %v procs", max, maxProcs))
		return 1, max, max / 2, nil
	}

	numCopiers = perProc * maxProcs
	// we try to leave one connection per-core for non-copier usages, otherwise using half the connections.
	if numCopiers > max-maxProcs {
		log.Warn("msg", fmt.Sprintf("had to reduce the number of copiers due to connection limits: wanted %v, reduced to %v", numCopiers, max/2))
		numCopiers = max / 2
	}

	return
}
