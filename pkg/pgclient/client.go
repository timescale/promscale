// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgclient

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/timescale/promscale/pkg/ha"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/health"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/promql"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/tenancy"
)

const (
	// defaultConnFraction is multipled with total connections to get
	// the max conns for a pool.
	defaultConnFraction = 0.5

	// defaultReaderFraction is the fraction of connections that should be
	// assigned to the reader pool, if Promscale is not in read-only mode.
	defaultReaderFraction = 0.3
)

// LockFunc does connect validation function, useful for things such as acquiring locks
// that should live the duration of the connection
type LockFunc = func(ctx context.Context, conn *pgx.Conn) error

// Client sends Prometheus samples to TimescaleDB
type Client struct {
	readerPool   pgxconn.PgxConn
	writerPool   pgxconn.PgxConn
	maintPool    pgxconn.PgxConn
	ingestor     ingestor.DBInserter
	querier      querier.Querier
	promqlEngine *promql.Engine
	healthCheck  health.HealthCheckerFn
	queryable    promql.Queryable
	metricCache  cache.MetricCache
	labelsCache  cache.LabelsCache
	seriesCache  cache.SeriesCache
	closePool    bool
	sigClose     chan struct{}
	haService    *ha.Service
}

// NewClient creates a new PostgreSQL client
func NewClient(r prometheus.Registerer, cfg *Config, mt tenancy.Authorizer, schemaLocker LockFunc, readOnly bool) (*Client, error) {
	var (
		err            error
		dbMaxConns     int
		readerPoolSize int
		writerPoolSize int
		maintPoolSize  int
		numCopiers     int
		writerPool     *pgxpool.Pool
		maintPool      *pgxpool.Pool
	)

	dbMaxConns, err = cfg.dbMaxConns()
	if err != nil {
		return nil, fmt.Errorf("max connections: %w", err)
	}

	readerPoolSize, writerPoolSize, maintPoolSize, err = cfg.GetPoolSizes(readOnly, dbMaxConns)
	if err != nil {
		return nil, err
	}

	maintPgConfig, err := cfg.getPgConfig(maintPoolSize)
	if err != nil {
		return nil, fmt.Errorf("get maint pg-config: %w", err)
	}
	maintPool, err = pgxpool.NewWithConfig(context.Background(), maintPgConfig)
	if err != nil {
		return nil, fmt.Errorf("err creating maintenance connection pool: %w", err)
	}

	if !readOnly {
		numCopiers, err = cfg.GetNumCopiers()
		if err != nil {
			return nil, fmt.Errorf("get num copiers: %w", err)
		}

		if numCopiers >= writerPoolSize {
			log.Warn("msg", "number of copiers greater than the writer-pool. Decreasing copiers to leave some connections for miscellaneous write tasks")
			numCopiers /= 2
		}

		writerPgConfig, err := cfg.getPgConfig(writerPoolSize)
		if err != nil {
			return nil, fmt.Errorf("get writer pg-config: %w", err)
		}
		writerPgConfig.AfterConnect = WriterPoolAfterConnect(schemaLocker, cfg.WriterSynchronousCommit)
		writerPool, err = pgxpool.NewWithConfig(context.Background(), writerPgConfig)
		if err != nil {
			return nil, fmt.Errorf("err creating writer connection pool: %w", err)
		}
	}

	readerPgConfig, err := cfg.getPgConfig(readerPoolSize)
	if err != nil {
		return nil, fmt.Errorf("get reader pg-config: %w", err)
	}

	statementCacheLog := "disabled"
	if cfg.EnableStatementsCache {
		statementCacheLog = "512" // Default pgx.
	}
	log.Info("msg", getRedactedConnStr(cfg.GetConnectionStr()))
	log.Info("msg", "runtime",
		"writer-pool.size", writerPoolSize,
		"reader-pool.size", readerPoolSize,
		"maint-pool.size", maintPoolSize,
		"min-pool.connections", MinPoolSize,
		"num-copiers", numCopiers,
		"statement-cache", statementCacheLog)

	readerPgConfig.AfterConnect = ReaderPoolAfterConnect(schemaLocker)
	readerPool, err := pgxpool.NewWithConfig(context.Background(), readerPgConfig)
	if err != nil {
		return nil, fmt.Errorf("err creating reader connection pool: %w", err)
	}
	client, err := NewClientWithPool(r, cfg, numCopiers, writerPool, readerPool, maintPool, mt, readOnly)
	if err != nil {
		return client, err
	}
	client.closePool = true
	return client, err
}

func ReaderPoolAfterConnect(schemaLocker LockFunc) func(context.Context, *pgx.Conn) error {
	return func(ctx context.Context, c *pgx.Conn) error {
		if schemaLocker != nil {
			err := schemaLocker(ctx, c)
			if err != nil {
				return err
			}
		}
		return model.RegisterCustomPgTypes(ctx, c)
	}
}

func WriterPoolAfterConnect(schemaLocker LockFunc, synchronousCommit bool) func(context.Context, *pgx.Conn) error {
	return func(ctx context.Context, conn *pgx.Conn) error {
		if !synchronousCommit {
			_, err := conn.Exec(ctx, "SET SESSION synchronous_commit to 'off'")
			if err != nil {
				return err
			}
		}

		if schemaLocker != nil {
			err := schemaLocker(ctx, conn)
			if err != nil {
				return err
			}
		}

		return model.RegisterCustomPgTypes(ctx, conn)
	}
}

func (cfg *Config) getPgConfig(poolSize int) (*pgxpool.Config, error) {
	min := MinPoolSize
	max := poolSize
	connStr := cfg.GetConnectionStr()

	pgConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}

	pgConfig.MaxConns = int32(max)
	pgConfig.MinConns = int32(min)

	if !cfg.EnableStatementsCache {
		// TODO use QueryExecModeExec
		pgConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	return pgConfig, nil
}

func getRedactedConnStr(s string) string {
	connURL, err := url.Parse(s)

	// Should never happen because we parsing the URL way before this
	// and error out if this happened.
	if err != nil {
		return "****"
	}

	if _, pwSet := connURL.User.Password(); pwSet {
		connURL.User = url.UserPassword(connURL.User.Username(), "****")
	}

	return connURL.String()
}

// NewClientWithPool creates a new PostgreSQL client with an existing connection pool.
func NewClientWithPool(r prometheus.Registerer, cfg *Config, numCopiers int, writerPool, readerPool, maintPool *pgxpool.Pool, mt tenancy.Authorizer, readOnly bool) (*Client, error) {
	sigClose := make(chan struct{})
	metricsCache := cache.NewMetricCache(cfg.CacheConfig)
	labelsCache := cache.NewLabelsCache(cfg.CacheConfig)
	seriesCache := cache.NewSeriesCache(cfg.CacheConfig, sigClose)
	invertedLabelsCache := cache.NewInvertedLabelsCache(cfg.CacheConfig, sigClose)

	c := ingestor.Cfg{
		NumCopiers:              numCopiers,
		IgnoreCompressedChunks:  cfg.IgnoreCompressedChunks,
		MetricsAsyncAcks:        cfg.MetricsAsyncAcks,
		TracesAsyncAcks:         cfg.TracesAsyncAcks,
		InvertedLabelsCacheSize: cfg.CacheConfig.InvertedLabelsCacheSize,
		TracesBatchTimeout:      cfg.TracesBatchTimeout,
		TracesMaxBatchSize:      cfg.TracesMaxBatchSize,
		TracesBatchWorkers:      cfg.TracesBatchWorkers,
	}

	var (
		writerConn pgxconn.PgxConn
		maintConn  pgxconn.PgxConn
	)

	readerConn := pgxconn.NewQueryLoggingPgxConn(readerPool)

	exemplarKeyPosCache := cache.NewExemplarLabelsPosCache(cfg.CacheConfig)

	labelsReader := lreader.NewLabelsReader(readerConn, labelsCache, mt.ReadAuthorizer())
	dbQuerier := querier.NewQuerier(readerConn, metricsCache, labelsReader, exemplarKeyPosCache, mt.ReadAuthorizer())
	queryable := query.NewQueryable(dbQuerier, labelsReader)

	dbIngestor := ingestor.DBInserter(ingestor.ReadOnlyIngestor{})
	if !readOnly {
		var err error
		writerConn = pgxconn.NewPgxConn(writerPool)
		dbIngestor, err = ingestor.NewPgxIngestor(writerConn, metricsCache, seriesCache, exemplarKeyPosCache, invertedLabelsCache, &c)
		if err != nil {
			log.Error("msg", "err starting the ingestor", "err", err)
			return nil, err
		}
	}
	if maintPool != nil {
		maintConn = pgxconn.NewPgxConn(maintPool)
	}

	client := &Client{
		readerPool:  readerConn,
		writerPool:  writerConn,
		maintPool:   maintConn,
		ingestor:    dbIngestor,
		querier:     dbQuerier,
		healthCheck: health.NewHealthChecker(readerConn),
		queryable:   queryable,
		metricCache: metricsCache,
		labelsCache: labelsCache,
		seriesCache: seriesCache,
		sigClose:    sigClose,
	}

	initMetrics(r, writerPool, readerPool, maintPool)
	return client, nil
}

func (c *Client) ReadOnlyConnection() pgxconn.PgxConn {
	return c.readerPool
}

func (c *Client) MaintenanceConnection() pgxconn.PgxConn {
	return c.maintPool
}

func (c *Client) InitPromQLEngine(cfg *query.Config) error {
	engine, err := query.NewEngine(log.GetLogger(), cfg.MaxQueryTimeout, cfg.LookBackDelta, cfg.SubQueryStepInterval, cfg.MaxSamples, cfg.EnabledFeatureMap)
	if err != nil {
		return fmt.Errorf("error creating PromQL engine: %w", err)
	}
	c.promqlEngine = engine
	return nil
}

func (c *Client) QueryEngine() *promql.Engine {
	return c.promqlEngine
}

// Close closes the client and performs cleanup
func (c *Client) Close() {
	log.Info("msg", "Shutting down Client")
	if c.ingestor != nil {
		c.ingestor.Close()
	}
	close(c.sigClose)
	if c.closePool {
		if c.maintPool != nil {
			c.maintPool.Close()
		}
		if c.writerPool != nil {
			c.writerPool.Close()
		}
		if c.readerPool != nil {
			c.readerPool.Close()
		}
	}
	if c.haService != nil {
		c.haService.Close()
	}
}

func (c *Client) Inserter() ingestor.DBInserter {
	return c.ingestor
}

// IngestMetrics writes the timeseries object into the DB
func (c *Client) IngestMetrics(ctx context.Context, r *prompb.WriteRequest) (uint64, uint64, error) {
	return c.ingestor.IngestMetrics(ctx, r)
}

// IngestTraces writes the traces object into the DB.
func (c *Client) IngestTraces(ctx context.Context, tr ptrace.Traces) error {
	return c.ingestor.IngestTraces(ctx, tr)
}

// Read returns the promQL query results
func (c *Client) Read(ctx context.Context, req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	if req == nil {
		return nil, nil
	}

	resp := prompb.ReadResponse{
		Results: make([]*prompb.QueryResult, len(req.Queries)),
	}

	qr := c.querier.RemoteReadQuerier(ctx)

	for i, q := range req.Queries {
		tts, err := qr.Query(q)
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
