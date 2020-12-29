// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"fmt"

	pgx "github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/ha"
	haClient "github.com/timescale/promscale/pkg/ha/client"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/health"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
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
	seriesCache   cache.SeriesCache
	closePool     bool
	sigClose      chan struct{}
	haService     *ha.Service
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

	var (
		pgConfig          *pgxpool.Config
		connectionArgsFmt string
	)
	if cfg.DbUri == defaultDBUri {
		connectionArgsFmt = "%s pool_max_conns=%d pool_min_conns=%d statement_cache_capacity=%d"
	} else {
		connectionArgsFmt = "%s&pool_max_conns=%d&pool_min_conns=%d&statement_cache_capacity=%d"
	}
	statementCacheCapacity := cfg.CacheConfig.MetricsCacheSize * 2
	connectionStringWithArgs := fmt.Sprintf(connectionArgsFmt, connectionStr, maxConnections, minConnections, statementCacheCapacity)
	pgConfig, err = pgxpool.ParseConfig(connectionStringWithArgs)
	if err != nil {
		log.Error("msg", "configuring connection", "err", util.MaskPassword(err.Error()))
		return nil, numCopiers, err
	}

	var statementCacheLog string
	if cfg.EnableStatementsCache {
		pgConfig.AfterRelease = observeStatementCacheState
		statementCacheEnabled.Set(1)
		statementCacheCap.Set(float64(statementCacheCapacity))
		statementCacheLog = fmt.Sprintf("%d statements", statementCacheCapacity)
	} else {
		log.Info("msg", "Statements cached disabled, using simple protocol for database connections.")
		pgConfig.ConnConfig.PreferSimpleProtocol = true
		statementCacheEnabled.Set(0)
		statementCacheCap.Set(0)
		statementCacheLog = "disabled"

	}
	log.Info("msg", util.MaskPassword(connectionStr),
		"numCopiers", numCopiers,
		"pool_max_conns", maxConnections,
		"pool_min_conns", minConnections,
		"statement_cache", statementCacheLog,
	)
	return pgConfig, numCopiers, nil
}

// NewClientWithPool creates a new PostgreSQL client with an existing connection pool.
func NewClientWithPool(cfg *Config, numCopiers int, dbConn pgxconn.PgxConn) (*Client, error) {
	sigClose := make(chan struct{})
	metricsCache := cache.NewMetricCache(cfg.CacheConfig)
	labelsCache := cache.NewLabelsCache(cfg.CacheConfig)
	seriesCache := cache.NewSeriesCache(cfg.CacheConfig, sigClose)
	c := ingestor.Cfg{
		AsyncAcks:      cfg.AsyncAcks,
		ReportInterval: cfg.ReportInterval,
		NumCopiers:     numCopiers,
	}

	var parser ingestor.Parser
	if cfg.HAEnabled {
		leaseClient := haClient.NewHaLeaseClient(dbConn)
		parser = ha.NewHAParser(ha.NewHAService(leaseClient), seriesCache)
	} else {
		parser = ingestor.DefaultParser(seriesCache)
	}
	dbIngestor, err := ingestor.NewPgxIngestor(dbConn, metricsCache, seriesCache, parser, &c)

	if err != nil {
		log.Error("msg", "err starting ingestor", "err", err)
		return nil, err
	}
	labelsReader := lreader.NewLabelsReader(dbConn, labelsCache)
	dbQuerier := querier.NewQuerier(dbConn, metricsCache, labelsReader)
	queryable := query.NewQueryable(dbQuerier, labelsReader)

	healthChecker := health.NewHealthChecker(dbConn)
	client := &Client{
		Connection:  dbConn,
		ingestor:    dbIngestor,
		querier:     dbQuerier,
		healthCheck: healthChecker,
		queryable:   queryable,
		metricCache: metricsCache,
		labelsCache: labelsCache,
		seriesCache: seriesCache,
		sigClose:    sigClose,
	}

	InitClientMetrics(client)
	return client, nil
}

// Close closes the client and performs cleanup
func (c *Client) Close() {
	log.Info("msg", "Shutting down Client")
	c.ingestor.Close()
	close(c.sigClose)
	if c.closePool {
		c.Connection.Close()
	}
	if c.haService != nil {
		c.haService.Close()
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

func observeStatementCacheState(conn *pgx.Conn) bool {
	// connections have been opened and are released already
	// but the Client metrics have not been initialized yet
	if statementCacheLen == nil {
		return true
	}
	statementCache := conn.StatementCache()
	if statementCache == nil {
		return true
	}

	statementCacheSize := statementCache.Len()
	statementCacheLen.Observe(float64(statementCacheSize))
	return true
}
