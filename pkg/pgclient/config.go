// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/limits"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/version"
)

// Config for the database.
type Config struct {
	CacheConfig            cache.Config
	AppName                string
	Host                   string
	Port                   int
	User                   string
	Password               string
	Database               string
	SslMode                string
	DbConnectionTimeout    time.Duration
	IgnoreCompressedChunks bool
	AsyncAcks              bool
	WriteConnections       int
	WriterPoolSize         int
	ReaderPoolSize         int
	MaxConnections         int
	UsesHA                 bool
	DbUri                  string
	EnableStatementsCache  bool
}

const (
	defaultDBUri             = ""
	defaultDBHost            = "localhost"
	defaultDBPort            = 5432
	defaultDBUser            = "postgres"
	defaultDBName            = "timescale"
	defaultDBPassword        = ""
	defaultSSLMode           = "require"
	defaultConnectionTime    = time.Minute
	defaultDbStatementsCache = true
	MinPoolSize              = 2
	defaultPoolSize          = -1
	defaultMaxConns          = -1
)

var (
	DefaultApp         = fmt.Sprintf("promscale@%s", version.Promscale)
	excessDBFlagsError = fmt.Errorf("failed to build DB credentials with provided flags. Please use either db flags or db-uri not both")
)

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(fs *flag.FlagSet, cfg *Config) *Config {
	cache.ParseFlags(fs, &cfg.CacheConfig)

	fs.StringVar(&cfg.AppName, "db.app", DefaultApp, "This sets the application_name in database connection string. "+
		"This is helpful during debugging when looking at pg_stat_activity.")
	fs.StringVar(&cfg.Host, "db.host", defaultDBHost, "Host for TimescaleDB/Vanilla Postgres.")
	fs.IntVar(&cfg.Port, "db.port", defaultDBPort, "TimescaleDB/Vanilla Postgres connection password.")
	fs.StringVar(&cfg.User, "db.user", defaultDBUser, "TimescaleDB/Vanilla Postgres user.")
	fs.StringVar(&cfg.Password, "db.password", defaultDBPassword, "Password for connecting to TimescaleDB/Vanilla Postgres.")
	fs.StringVar(&cfg.Database, "db.name", defaultDBName, "Database name.")
	fs.StringVar(&cfg.SslMode, "db.ssl-mode", defaultSSLMode, "TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass 'allow' as value.")
	fs.DurationVar(&cfg.DbConnectionTimeout, "db.connection-timeout", defaultConnectionTime, "Timeout for establishing the connection between Promscale and TimescaleDB.")
	fs.BoolVar(&cfg.IgnoreCompressedChunks, "metrics.ignore-samples-written-to-compressed-chunks", false, "Ignore/drop samples that are being written to compressed chunks. "+
		"Setting this to false allows Promscale to ingest older data by decompressing chunks that were earlier compressed. "+
		"However, setting this to true will save your resources that may be required during decompression. ")
	fs.IntVar(&cfg.WriteConnections, "db.connections.num-writers", 0, "Number of database connections for writing metrics to database. "+
		"By default, this will be set based on the number of CPUs available to the DB Promscale is connected to.")
	fs.IntVar(&cfg.WriterPoolSize, "db.connections.writer-pool.size", defaultPoolSize, "Maximum size of the writer pool of database connections. This defaults to 50% of max_connections "+
		"allowed by the database.")
	fs.IntVar(&cfg.ReaderPoolSize, "db.connections.reader-pool.size", defaultPoolSize, "Maximum size of the reader pool of database connections. This defaults to 30% of max_connections "+
		"allowed by the database.")
	fs.IntVar(&cfg.MaxConnections, "db.connections-max", defaultMaxConns, "Maximum number of connections to the database that should be opened at once. "+
		"It defaults to 80% of the maximum connections that the database can handle. ")
	fs.StringVar(&cfg.DbUri, "db.uri", defaultDBUri, "TimescaleDB/Vanilla Postgres DB URI. "+
		"Example DB URI `postgres://postgres:password@localhost:5432/timescale?sslmode=require`")
	fs.BoolVar(&cfg.EnableStatementsCache, "db.statements-cache", defaultDbStatementsCache, "Whether database connection pool should use cached prepared statements. "+
		"Disable if using PgBouncer")
	fs.BoolVar(&cfg.AsyncAcks, "metrics.async-acks", false, "Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss.")
	return cfg
}

func Validate(cfg *Config, lcfg limits.Config) error {
	if err := cfg.validateConnectionSettings(); err != nil {
		return err
	}
	return cache.Validate(&cfg.CacheConfig, lcfg)
}

// validateConnectionSettings checks that we are not using both a DB URI and
// DB configuration flags
func (cfg Config) validateConnectionSettings() error {
	// If we are using DB URI, nothing to check.
	if cfg.DbUri == defaultDBUri {
		return nil
	}

	// If using DB URI, check if any DB flags are supplied.
	if cfg.AppName != DefaultApp ||
		cfg.Database != defaultDBName ||
		cfg.Host != defaultDBHost ||
		cfg.Port != defaultDBPort ||
		cfg.User != defaultDBUser ||
		cfg.Password != defaultDBPassword ||
		cfg.SslMode != defaultSSLMode ||
		cfg.DbConnectionTimeout != defaultConnectionTime {
		return excessDBFlagsError
	}

	return nil
}

// GetConnectionStr returns a Postgres connection string
func (cfg *Config) GetConnectionStr() string {
	// If DB URI is not supplied, generate one from DB flags.
	if cfg.DbUri == defaultDBUri {
		v := url.Values{}
		v.Set("application_name", cfg.AppName)
		v.Set("sslmode", cfg.SslMode)
		v.Set("connect_timeout", fmt.Sprintf("%.f", cfg.DbConnectionTimeout.Seconds()))
		u := url.URL{
			Scheme: "postgresql",
			User:   url.UserPassword(cfg.User, cfg.Password),
			Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
			Path:   cfg.Database, RawQuery: v.Encode(),
		}
		return u.String()
	}
	return cfg.DbUri
}

// GetPoolSize returns the max pool size based on the max_connections allowed by database and the defaultFraction.
// Arg inputPoolSize is the pool size provided in the CLI flag.
func (cfg *Config) GetPoolSize(poolName string, defaultFraction float64, inputPoolSize int) (int, error) {
	maxConns, err := cfg.maxConn()
	if err != nil {
		return 0, fmt.Errorf("max connections: %w", err)
	}

	if inputPoolSize == defaultPoolSize {
		// For the default case, we need to take up defaultFraction of allowed connections.
		poolSize := float64(maxConns) * defaultFraction
		if cfg.UsesHA {
			poolSize /= 2
		}
		return int(poolSize), nil
	}

	// Custom pool size,
	if inputPoolSize < MinPoolSize {
		return 0, fmt.Errorf("%s pool size canot be less than %d: received %d", poolName, MinPoolSize, inputPoolSize)
	} else if inputPoolSize > maxConns {
		return 0, fmt.Errorf("%s pool size canot be greater than the 'max_connections' allowed by the database", poolName)
	}
	if cfg.UsesHA {
		inputPoolSize /= 2
	}
	return inputPoolSize, nil
}

func (cfg *Config) maxConn() (int, error) {
	conn, err := pgx.Connect(context.Background(), cfg.GetConnectionStr())
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	var maxConns int
	err = conn.QueryRow(context.Background(), "SELECT current_setting('max_connections')::int").Scan(&maxConns)
	if err != nil {
		return 0, fmt.Errorf("error getting 'max_connections': %w", err)
	}
	return maxConns, nil
}

func (cfg *Config) GetNumCopiers() (int, error) {
	if cfg.WriteConnections > 0 {
		return cfg.WriteConnections, nil
	}

	conn, err := pgx.Connect(context.Background(), cfg.GetConnectionStr())
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	numCopiers := 0
	err = conn.QueryRow(context.Background(), "SELECT _prom_ext.num_cpus()").Scan(&numCopiers)
	if err != nil {
		return 0, fmt.Errorf("error fetching number of CPUs from extension: %w", err)
	}

	return numCopiers, nil
}
