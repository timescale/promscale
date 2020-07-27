package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/timescale/timescale-prometheus/pkg/prompb"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/query"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

// Config for the database
type Config struct {
	host             string
	port             int
	user             string
	password         string
	database         string
	sslMode          string
	dbConnectRetries int
	AsyncAcks        bool
	ReportInterval   int
	LabelsCacheSize  uint64
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
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config, readHist prometheus.ObserverVec) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()

	maxProcs := runtime.GOMAXPROCS(-1)
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	if maxProcs <= 0 {
		maxProcs = 1
	}
	connectionPool, err := pgxpool.Connect(context.Background(), connectionStr+fmt.Sprintf(" pool_max_conns=%d pool_min_conns=%d", maxProcs*pgmodel.ConnectionsPerProc, maxProcs))

	log.Info("msg", util.MaskPassword(connectionStr))

	if err != nil {
		log.Error("err creating connection pool for new client", util.MaskPassword(err.Error()))
		return nil, err
	}

	metrics, _ := bigcache.NewBigCache(pgmodel.DefaultCacheConfig())
	cache := &pgmodel.MetricNameCache{Metrics: metrics}

	c := pgmodel.Cfg{AsyncAcks: cfg.AsyncAcks, ReportInterval: cfg.ReportInterval}
	ingestor, err := pgmodel.NewPgxIngestorWithMetricCache(connectionPool, cache, &c)
	if err != nil {
		log.Error("err starting ingestor", err)
		return nil, err
	}
	reader := pgmodel.NewPgxReaderWithMetricCache(connectionPool, cache, cfg.LabelsCacheSize)

	queryable := query.NewQueryable(reader.GetQuerier())

	return &Client{Connection: connectionPool, ingestor: ingestor, reader: reader, queryable: queryable, cfg: cfg}, nil
}

// GetConnectionStr returns a Postgres connection string
func (cfg *Config) GetConnectionStr() string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.sslMode)
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

// HealthCheck checks that the client is properly connected
func (c *Client) HealthCheck() error {
	return c.reader.HealthCheck()
}

// GetQueryable returns the Prometheus storage.Queryable interface thats running
// with the same underlying Querier as the DBReader.
func (c *Client) GetQueryable() *query.Queryable {
	return c.queryable
}
