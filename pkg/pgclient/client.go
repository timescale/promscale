// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"fmt"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/health"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/util"
)

// Client sends Prometheus samples to TimescaleDB
type Client struct {
	Connection    pgxconn.PgxConn
	ingestor      *ingestor.DBIngestor
	querier       querier.Querier
	healthCheck   health.HealthCheckerFn
	queryable     promql.Queryable
	ConnectionStr string
	metricCache   cache.MetricCache
	labelsCache   cache.LabelsCache
	closePool     bool
}

// Post connect validation function, useful for things such as acquiring locks
// that should live the duration of the connection
type LockFunc = func(ctx context.Context, conn *pgx.Conn) error

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config, schemaLocker LockFunc) (*Client, error) {
	pgConfig, numCopiers, err := getPgConfig(cfg)
	if err != nil {
		return nil, err
	}

	pgConfig.AfterConnect = schemaLocker
	connectionPool, err := pgxpool.ConnectConfig(context.Background(), pgConfig)
	if err != nil {
		log.Error("msg", "err creating connection pool for new client", "err", util.MaskPassword(err.Error()))
		return nil, err
	}

	dbConn := pgxconn.NewPgxConn(connectionPool)
	client, err := NewClientWithPool(cfg, numCopiers, dbConn)
	if err != nil {
		return client, err
	}
	client.closePool = true
	return client, err
}

func getPgConfig(cfg *Config) (*pgxpool.Config, int, error) {
	connectionStr, err := cfg.GetConnectionStr()
	if err != nil {
		return nil, 0, err
	}

	minConnections, maxConnections, numCopiers, err := cfg.GetNumConnections()
	if err != nil {
		log.Error("msg", "configuring number of connections", "err", util.MaskPassword(err.Error()))
		return nil, numCopiers, err
	}

	var pgConfig *pgxpool.Config
	if cfg.DbUri == defaultDBUri {
		pgConfig, err = pgxpool.ParseConfig(connectionStr + fmt.Sprintf(" pool_max_conns=%d pool_min_conns=%d", maxConnections, minConnections))
	} else {
		pgConfig, err = pgxpool.ParseConfig(connectionStr + fmt.Sprintf("&pool_max_conns=%d&pool_min_conns=%d", maxConnections, minConnections))
	}

	if err != nil {
		log.Error("msg", "configuring connection", "err", util.MaskPassword(err.Error()))
		return nil, numCopiers, err
	}

	log.Info("msg", util.MaskPassword(connectionStr), "numCopiers", numCopiers, "pool_max_conns", maxConnections, "pool_min_conns", minConnections)
	return pgConfig, numCopiers, nil
}

// NewClientWithPool creates a new PostgreSQL client with an existing connection pool.
func NewClientWithPool(cfg *Config, numCopiers int, dbConn pgxconn.PgxConn) (*Client, error) {
	metricsCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cfg.MetricsCacheSize)}
	labelsCache := clockcache.WithMax(cfg.LabelsCacheSize)
	c := ingestor.Cfg{
		AsyncAcks:       cfg.AsyncAcks,
		ReportInterval:  cfg.ReportInterval,
		SeriesCacheSize: cfg.SeriesCacheSize,
		NumCopiers:      numCopiers,
	}
	ingestor, err := ingestor.NewPgxIngestorWithMetricCache(dbConn, metricsCache, &c)
	if err != nil {
		log.Error("msg", "err starting ingestor", "err", err)
		return nil, err
	}
	labelsReader := model.NewLabelsReader(dbConn, labelsCache)
	querier := querier.NewQuerier(dbConn, metricsCache, labelsReader)
	queryable := query.NewQueryable(querier, labelsReader)

	healthChecker := health.NewHealthChecker(dbConn)
	client := &Client{
		Connection:  dbConn,
		ingestor:    ingestor,
		querier:     querier,
		healthCheck: healthChecker,
		queryable:   queryable,
		metricCache: metricsCache,
		labelsCache: labelsCache,
	}

	InitClientMetrics(client)

	return client, nil
}

// Close closes the client and performs cleanup
func (c *Client) Close() {
	log.Info("msg", "Shutting down Client")
	c.ingestor.Close()
	if c.closePool {
		c.Connection.Close()
	}
}

// Ingest writes the timeseries object into the DB
func (c *Client) Ingest(tts []prompb.TimeSeries, req *prompb.WriteRequest) (uint64, error) {
	return c.ingestor.Ingest(tts, req)
}

// Read returns the promQL query results
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	if req == nil {
		return nil, nil
	}

	resp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	for i, q := range req.Queries {
		tts, err := c.querier.Query(q)
		if err != nil {
			return nil, err
		}
		resp.Results[i] = &prompb.QueryResult{
			Timeseries: tts,
		}
	}

	return &resp, nil
}

func (c *Client) NumCachedMetricNames() int {
	return c.metricCache.Len()
}

func (c *Client) MetricNamesCacheCapacity() int {
	return c.metricCache.Cap()
}

func (c *Client) NumCachedLabels() int {
	return c.labelsCache.Len()
}

func (c *Client) LabelsCacheCapacity() int {
	return c.labelsCache.Cap()
}

// HealthCheck checks that the client is properly connected
func (c *Client) HealthCheck() error {
	return c.healthCheck()
}

// Queryable returns the Prometheus promql.Queryable interface that's running
// with the same underlying Querier as the Client.
func (c *Client) Queryable() promql.Queryable {
	return c.queryable
}
