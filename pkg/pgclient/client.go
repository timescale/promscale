package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strconv"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/prompb"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/util"
)

// Config for the database
type Config struct {
	Host                    string
	Port                    int
	User                    string
	Password                string
	Database                string
	SslMode                 string
	DbConnectRetries        int
	AsyncAcks               bool
	ReportInterval          int
	LabelsCacheSize         uint64
	MetricsCacheSize        uint64
	SeriesCacheSize         uint64
	WriteConnectionsPerProc int
	MaxConnections          int
}

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.Host, "db-host", "localhost", "Host for TimescaleDB/Vanilla Postgres.")
	flag.IntVar(&cfg.Port, "db-port", 5432, "TimescaleDB/Vanilla Postgres connection password.")
	flag.StringVar(&cfg.User, "db-user", "postgres", "TimescaleDB/Vanilla Postgres user.")
	flag.StringVar(&cfg.Password, "db-password", "", "Password for connecting to TimescaleDB/Vanilla Postgres.")
	flag.StringVar(&cfg.Database, "db-name", "timescale", "Database name.")
	flag.StringVar(&cfg.SslMode, "db-ssl-mode", "require", "TimescaleDB/Vanilla Postgres connection ssl mode. If you do not want to use ssl, pass 'allow' as value.")
	flag.IntVar(&cfg.DbConnectRetries, "db-connect-retries", 0, "Number of retries Promscale should make for establishing connection with the database.")
	flag.BoolVar(&cfg.AsyncAcks, "async-acks", false, "Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss.")
	flag.IntVar(&cfg.ReportInterval, "tput-report", 0, "Interval in seconds at which throughput should be reported.")
	flag.Uint64Var(&cfg.LabelsCacheSize, "labels-cache-size", 10000, "Maximum number of labels to cache.")
	flag.Uint64Var(&cfg.MetricsCacheSize, "metrics-cache-size", pgmodel.DefaultMetricCacheSize, "Maximum number of metric names to cache.")
	flag.IntVar(&cfg.WriteConnectionsPerProc, "db-writer-connection-concurrency", 4, "Maximum number of database connections for writing per go process.")
	flag.IntVar(&cfg.MaxConnections, "db-connections-max", -1, "Maximum number of connections to the database that should be opened at once. It defaults to 80% of the maximum connections that the database can handle.")
	return cfg
}

// Client sends Prometheus samples to TimescaleDB
type Client struct {
	Connection    *pgxpool.Pool
	ingestor      *pgmodel.DBIngestor
	reader        *pgmodel.DBReader
	queryable     *query.Queryable
	cfg           *Config
	ConnectionStr string
	metricCache   *pgmodel.MetricNameCache
}

// Post connect validation function, useful for things such as acquiring locks
// that should live the duration of the connection
type LockFunc = func(ctx context.Context, conn *pgx.Conn) error

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config, schemaLocker LockFunc) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()
	minConnections, maxConnections, numCopiers, err := cfg.GetNumConnections()
	if err != nil {
		log.Error("msg", "configuring number of connections", "err", util.MaskPassword(err.Error()))
		return nil, err
	}

	pgConfig, err := pgxpool.ParseConfig(connectionStr + fmt.Sprintf(" pool_max_conns=%d pool_min_conns=%d", maxConnections, minConnections))
	if err != nil {
		log.Error("msg", "configuring connection", "err", util.MaskPassword(err.Error()))
	}

	pgConfig.AfterConnect = schemaLocker
	connectionPool, err := pgxpool.ConnectConfig(context.Background(), pgConfig)

	log.Info("msg", util.MaskPassword(connectionStr), "numCopiers", numCopiers, "pool_max_conns", maxConnections, "pool_min_conns", minConnections)

	if err != nil {
		log.Error("msg", "err creating connection pool for new client", "err", util.MaskPassword(err.Error()))
		return nil, err
	}

	return NewClientWithPool(cfg, numCopiers, connectionPool)
}

// NewClientWithPool creates a new PostgreSQL client with an existing connection pool.
func NewClientWithPool(cfg *Config, numCopiers int, pool *pgxpool.Pool) (*Client, error) {
	cache := &pgmodel.MetricNameCache{Metrics: clockcache.WithMax(cfg.MetricsCacheSize)}

	c := pgmodel.Cfg{
		AsyncAcks:       cfg.AsyncAcks,
		ReportInterval:  cfg.ReportInterval,
		SeriesCacheSize: cfg.SeriesCacheSize,
		NumCopiers:      numCopiers,
	}
	ingestor, err := pgmodel.NewPgxIngestorWithMetricCache(pool, cache, &c)
	if err != nil {
		log.Error("msg", "err starting ingestor", "err", err)
		return nil, err
	}
	reader := pgmodel.NewPgxReaderWithMetricCache(pool, cache, cfg.LabelsCacheSize)

	queryable := query.NewQueryable(reader.GetQuerier())

	client := &Client{
		Connection:  pool,
		ingestor:    ingestor,
		reader:      reader,
		queryable:   queryable,
		cfg:         cfg,
		metricCache: cache,
	}

	InitClientMetrics(client)

	return client, nil
}

// GetConnectionStr returns a Postgres connection string
func (cfg *Config) GetConnectionStr() string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.Host, cfg.Port, cfg.User, cfg.Database, cfg.Password, cfg.SslMode)
}

func (cfg *Config) GetNumConnections() (min int, max int, numCopiers int, err error) {
	maxProcs := runtime.GOMAXPROCS(-1)
	if cfg.WriteConnectionsPerProc < 1 {
		return 0, 0, 0, fmt.Errorf("invalid number of connections-per-proc %v, must be at least 1", cfg.WriteConnectionsPerProc)
	}
	perProc := cfg.WriteConnectionsPerProc
	max = cfg.MaxConnections
	if max < 1 {
		conn, err := pgx.Connect(context.Background(), cfg.GetConnectionStr())
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

// Close closes the client and performs cleanup
func (c *Client) Close() {
	log.Info("msg", "Shutting down Client")
	c.ingestor.Close()
	c.Connection.Close()
}

// Ingest writes the timeseries object into the DB
func (c *Client) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	return c.ingestor.Ingest(tts, req)
}

// Read returns the promQL query results
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	return c.reader.Read(req)
}

func (c *Client) NumCachedMetricNames() int {
	return c.metricCache.NumElements()
}

func (c *Client) MetricNamesCacheCapacity() int {
	return c.metricCache.Capacity()
}

func (c *Client) NumCachedLabels() int {
	return c.reader.GetQuerier().NumCachedLabels()
}

func (c *Client) LabelsCacheCapacity() int {
	return c.reader.GetQuerier().LabelsCacheCapacity()
}

// HealthCheck checks that the client is properly connected
func (c *Client) HealthCheck() error {
	return c.reader.HealthCheck()
}

// GetQueryable returns the Prometheus storage.Queryable interface thats running
// with the same underlying Querier as the DBReader.
func (c *Client) GetQueryable() *query.Queryable {
	return c.queryable
}
