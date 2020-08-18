package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"

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
	flag.StringVar(&cfg.sslMode, "db-ssl-mode", "disable", "The TimescaleDB connection ssl mode")
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

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config, readHist prometheus.ObserverVec) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()
	minConnections, maxConnections, numCopiers, err := cfg.GetNumConnections()
	if err != nil {
		log.Error("err configuring number of connections", util.MaskPassword(err.Error()))
		return nil, err
	}
	connectionPool, err := pgxpool.Connect(context.Background(), connectionStr+fmt.Sprintf(" pool_max_conns=%d pool_min_conns=%d", maxConnections, minConnections))

	log.Info("msg", util.MaskPassword(connectionStr), "numCopiers", numCopiers)

	if err != nil {
		log.Error("err creating connection pool for new client", util.MaskPassword(err.Error()))
		return nil, err
	}

	cache := &pgmodel.MetricNameCache{Metrics: clockcache.WithMax(cfg.MetricsCacheSize)}

	c := pgmodel.Cfg{
		AsyncAcks:       cfg.AsyncAcks,
		ReportInterval:  cfg.ReportInterval,
		SeriesCacheSize: cfg.SeriesCacheSize,
		NumCopiers:      numCopiers,
	}
	ingestor, err := pgmodel.NewPgxIngestorWithMetricCache(connectionPool, cache, &c)
	if err != nil {
		log.Error("err starting ingestor", err)
		return nil, err
	}
	reader := pgmodel.NewPgxReaderWithMetricCache(connectionPool, cache, cfg.LabelsCacheSize)

	queryable := query.NewQueryable(reader.GetQuerier())

	return &Client{
		Connection:  connectionPool,
		ingestor:    ingestor,
		reader:      reader,
		queryable:   queryable,
		cfg:         cfg,
		metricCache: cache,
	}, nil
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
		row := conn.QueryRow(context.Background(), "SHOW max_connections")
		err = row.Scan(&max)
		if err != nil {
			return 0, 0, 0, err
		}
		if max <= 1 {
			log.Warn("msg", "database can only handle 1 connection")
			return 1, 1, 1, nil
		}
		// we try to only use 80% the database connections
		max = int(0.8 * float32(max))
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
	// we leave one connection per-core for non-copier usages
	if numCopiers+maxProcs > max {
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
