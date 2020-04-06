package pgclient

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/timescale/timescale-prometheus/pkg/util"
	"regexp"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/prometheus/prometheus/prompb"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
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
}

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.host, "pg-host", "localhost", "The PostgreSQL host")
	flag.IntVar(&cfg.port, "pg-port", 5432, "The PostgreSQL port")
	flag.StringVar(&cfg.user, "pg-user", "postgres", "The PostgreSQL user")
	flag.StringVar(&cfg.password, "pg-password", "", "The PostgreSQL password")
	flag.StringVar(&cfg.database, "pg-database", "postgres", "The PostgreSQL database")
	flag.StringVar(&cfg.sslMode, "pg-ssl-mode", "disable", "The PostgreSQL connection ssl mode")
	flag.IntVar(&cfg.dbConnectRetries, "pg-db-connect-retries", 0, "How many times to retry connecting to the database")
	return cfg
}

// Client sends Prometheus samples to PostgreSQL
type Client struct {
	Connection    *pgxpool.Pool
	ingestor      *pgmodel.DBIngestor
	reader        *pgmodel.DBReader
	cfg           *Config
	ConnectionStr string
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()

	connectionPool, err := pgxpool.Connect(context.Background(), connectionStr)

	escapedConnStr := regexp.MustCompile("password='(.+?)'").ReplaceAllLiteralString(connectionStr, "password='****'")
	log.Info("msg", escapedConnStr)

	if err != nil {
		log.Error("msg", "err creating connection pool for new client", err)
		return nil, err
	}

	config := bigcache.DefaultConfig(10 * time.Minute)
	metrics, _ := bigcache.NewBigCache(config)
	cache := &pgmodel.MetricNameCache{Metrics: metrics}

	ingestor := pgmodel.NewPgxIngestorWithMetricCache(connectionPool, cache)
	reader := pgmodel.NewPgxReaderWithMetricCache(connectionPool, cache)

	return &Client{Connection: connectionPool, ingestor: ingestor, reader: reader, cfg: cfg}, nil
}

func OnDbReady(cfg *Config) util.Eventual {
	numRetries := uint(10)
	backOff := 10 * time.Second
	isDbReady := func() (interface{}, error) {
		dbStd, err := sql.Open("pgx", cfg.GetConnectionStr())
		if err != nil {
			err = fmt.Errorf("db was not ready: %s", err)
			log.Warn("msg", err)
			return nil, err
		}
		defer func() {
			err := dbStd.Close()
			if err != nil {
				log.Error("msg", fmt.Errorf("error closing conn in db readiness check: %s", err))
			}
		}()
		row := dbStd.QueryRow("SELECT 1")
		var one int
		if err := row.Scan(&one); err != nil {
			err = fmt.Errorf("db was not ready: %s", err)
			log.Warn("msg", err)
			return nil, err
		}
		return true, nil
	}
	return util.NewEventualWith("is-db-alive-yet", numRetries, backOff, isDbReady)
}

func NewClientProvider(cfg *Config, onDbReady util.Eventual) util.Eventual {
	return onDbReady.Map("create-db-client", func(dbOk interface{}) interface{} {
		if dbOk == nil {
			// db was not ok
			return nil
		}
		client, err := NewClient(cfg)
		if err != nil {
			return nil
		}
		return client
	})
}

func (cfg *Config) GetConnectionStr() string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.sslMode)
}

func (c *Client) Ingest(tts []prompb.TimeSeries) (uint64, error) {
	return c.ingestor.Ingest(tts)
}

// Read returns the promQL query results
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	return c.reader.Read(req)
}

// HealthCheck checks that the client is properly connected
func (c *Client) HealthCheck() error {
	return c.reader.HealthCheck()
}
