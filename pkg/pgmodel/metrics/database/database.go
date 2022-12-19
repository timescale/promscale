package database

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type Engine interface {
	Run() error
	IsRunning() bool
}

type metricsEngineImpl struct {
	conn      pgxconn.PgxConn
	ctx       context.Context
	isRunning atomic.Value
	metrics   []metricQueryWrap
}

// NewEngine creates an engine that performs database metrics evaluation every evalInterval.
// The engine runs predefined queries that returns a BIGINT as the metric value
// which is then set as a value to Prometheus metric.
//
// Note: Make sure to call this only when the database is TimescaleDB. Plain Postgres
// will cause evaluation errors.
func NewEngine(ctx context.Context, conn pgxconn.PgxConn) *metricsEngineImpl {
	engine := &metricsEngineImpl{
		conn:    conn,
		ctx:     ctx,
		metrics: metrics,
	}
	engine.isRunning.Store(false)
	return engine
}

func (e *metricsEngineImpl) register() {
	prometheus.MustRegister(getMetrics(e.metrics)...)
}

func (e *metricsEngineImpl) unregister() {
	m := getMetrics(e.metrics)
	for i := range m {
		prometheus.Unregister(m[i])
	}
}

func getMetrics(m []metricQueryWrap) []prometheus.Collector {
	var metrics []prometheus.Collector
	for i := range m {
		metrics = append(metrics, m[i].metrics...)
	}
	return metrics
}

const (
	timeout        = time.Minute
	evalInterval   = time.Minute * 3
	maxRetryWait   = time.Minute * 30
	pauseThreshold = 5
)

func (e *metricsEngineImpl) Run() error {
	if e.isRunning.Load().(bool) {
		return fmt.Errorf("cannot run the engine: database metrics is already running")
	}
	e.register()
	e.isRunning.Store(true)
	go func() {
		timeoutCount := 0
		defer func() {
			e.isRunning.Store(false)
			upMetric.Set(0)
		}()
		wait := evalInterval
		for {
			if err := e.Update(); err != nil {
				// Consider any error as timeout since we want to limit db executions if updating metrics is erroring out.
				timeoutCount++
			} else {
				// This is to reset the state of engine that was earlier prepared for handling database timeout.
				// The moment the db is happy with the timeout (when there is no longer a timeout), hence
				// we resume the normal wait operation and reset the timeout so that previous timeouts do not affect
				// the next iterations.
				timeoutCount = 0
				wait = evalInterval
				upMetric.Set(1)
			}
			if timeoutCount > pauseThreshold {
				log.Warn("msg", fmt.Sprintf("Database metric evaluation taking more time than expected. Pausing evaluation for %.0f minutes", wait.Minutes()))
				timeoutCount = 0
				wait = getBackoff(wait)
				upMetric.Set(0)
			}
			select {
			case <-e.ctx.Done():
				e.unregister()
				return
			case <-time.After(wait):
			}
		}
	}()
	return nil
}

// Update blocks until all db metrics are updated. This can be useful in E2E test when we want to avoid concurrent behaviour.
func (e *metricsEngineImpl) Update() error {
	batch := e.conn.NewBatch()
	batchMetrics := []metricQueryWrap{}
	for i, m := range e.metrics {
		if m.isHealthCheck {
			healthCheck(e.conn, m)
			continue
		}
		now := time.Now()
		if m.customPollConfig.enabled && now.Sub(m.customPollConfig.lastUpdate) < m.customPollConfig.interval {
			continue
		} else if m.customPollConfig.enabled {
			e.metrics[i].customPollConfig.lastUpdate = now
		}
		batch.Queue(m.query)
		batchMetrics = append(batchMetrics, m)
	}

	batchCtx, cancelBatch := context.WithTimeout(e.ctx, timeout)
	defer cancelBatch()

	results, err := e.conn.SendBatch(batchCtx, batch)
	if err != nil {
		log.Warn("msg", "error evaluating the database metrics batch", "err", err.Error())
		return err
	}
	if batchCtx.Err() != nil {
		log.Warn("msg", "context error while evaluating the database metrics batch", "err", batchCtx.Err().Error())
		return err
	}
	handleResults(results, batchMetrics)
	return results.Close()
}

func healthCheck(conn pgxconn.PgxConn, healthMetric metricQueryWrap) {
	val := 0

	start := time.Now()
	err := conn.QueryRow(context.Background(), healthMetric.query).Scan(&val)
	networkLatency := time.Since(start).Milliseconds()

	updateMetric(healthMetric.metrics[0], 1)
	if err != nil {
		dbHealthErrors.Inc()
		log.Error("msg", "health check failed", "err", err.Error())
		// Important to set to -ve, otherwise if the connection is lost, latency will keep
		// showing last latency of successful connection, if we choose to not update during
		// an error.
		networkLatency = -1
	}
	dbNetworkLatency.Set(float64(networkLatency))
}

// getBackoff returns a conditional backoff duration that is an exponential increment.
func getBackoff(previous time.Duration) time.Duration {
	now := previous * 2
	if now > maxRetryWait {
		now = maxRetryWait
	}
	return now
}

func (e *metricsEngineImpl) IsRunning() bool {
	return e.isRunning.Load().(bool)
}

func handleResults(results pgx.BatchResults, m []metricQueryWrap) {
	for i := range m {
		entry := m[i]
		valsCount := len(entry.metrics)
		vals := make([]interface{}, valsCount)
		for vi := range vals {
			vals[vi] = new(int64)
		}
		err := results.QueryRow().Scan(vals...)
		if err != nil {
			log.Warn("msg", fmt.Sprintf("error evaluating database metric with query: %s", entry.query), "err", err.Error())
			return
		}
		var value int64
		for vi := range vals {
			if vals[vi] != nil {
				value = *vals[vi].(*int64)
			}
			updateMetric(entry.metrics[vi], value)
		}
	}
}

func updateMetric(m prometheus.Collector, value int64) {
	switch n := m.(type) {
	case prometheus.Gauge: // Keep Gauge above Counter, since Gauge satisfies Counter interface but not vice-versa.
		n.Set(float64(value))
	case prometheus.Counter:
		n.Add(float64(value))
	case prometheus.Histogram:
		n.Observe(float64(value))
	default:
		panic(fmt.Sprintf("metric %s is of type %T", m, m))
	}
}
