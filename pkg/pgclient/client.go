package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strconv"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/timescale/timescale-prometheus/pkg/clockcache"
	"github.com/timescale/timescale-prometheus/pkg/prompb"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

// Config for the database
type Config struct {
	host                    string
	port                    int
	user                    string
	password                string
	database                string
	sslMode                 string
	dbConnectRetries        int
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
	flag.StringVar(&cfg.host, "db-host", "localhost", "The TimescaleDB host")
	flag.IntVar(&cfg.port, "db-port", 5432, "The TimescaleDB port")
	flag.StringVar(&cfg.user, "db-user", "postgres", "The TimescaleDB user")
	flag.StringVar(&cfg.password, "db-password", "", "The TimescaleDB password")
	flag.StringVar(&cfg.database, "db-name", "timescale", "The TimescaleDB database")
	flag.StringVar(&cfg.sslMode, "db-ssl-mode", "require", "The TimescaleDB connection ssl mode")
	flag.IntVar(&cfg.dbConnectRetries, "db-connect-retries", 0, "How many times to retry connecting to the database")
	flag.BoolVar(&cfg.AsyncAcks, "async-acks", false, "Ack before data is written to DB")
	flag.IntVar(&cfg.ReportInterval, "tput-report", 0, "interval in seconds at which throughput should be reported")
	flag.Uint64Var(&cfg.LabelsCacheSize, "labels-cache-size", 10000, "maximum number of labels to cache")
	flag.Uint64Var(&cfg.MetricsCacheSize, "metrics-cache-size", pgmodel.DefaultMetricCacheSize, "maximum number of metric names to cache")
	flag.IntVar(&cfg.WriteConnectionsPerProc, "db-writer-connection-concurrency", 4, "maximum number of database connections per go process writing to the database")
	flag.IntVar(&cfg.MaxConnections, "db-connections-max", -1, "maximum connections that can be open at once, defaults to 80% of the max the DB can handle")
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
		log.Error("err creating connection pool for new client", util.MaskPassword(err.Error()))
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
		log.Error("err starting ingestor", err)
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
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.sslMode)
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
	c.ingestor.Close()
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
