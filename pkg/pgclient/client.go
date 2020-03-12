package pgclient

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"regexp"

	pgx "github.com/jackc/pgx/v4"
	"github.com/prometheus/prometheus/prompb"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/pgmodel"
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
	Connection    *pgx.Conn
	ingestor      *pgmodel.DBIngestor
	cfg           *Config
	ConnectionStr string
}

var (
	createTmpTableStmt *sql.Stmt
)

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()

	connection, err := pgx.Connect(context.Background(), connectionStr)

	log.Info("msg", regexp.MustCompile("password='(.+?)'").ReplaceAllLiteralString(connectionStr, "password='****'"))

	if err != nil {
		log.Error("err", err)
		return nil, err
	}

	ingestor := pgmodel.NewPgxIngestor(connection)

	return &Client{Connection: connection, ingestor: ingestor, cfg: cfg}, nil
}

func (cfg *Config) GetConnectionStr() string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.sslMode)
}

func (c *Client) Ingest(tts []prompb.TimeSeries) (uint64, error) {
	return c.ingestor.Ingest(tts)
}
