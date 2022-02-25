package database

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type Engine interface {
	Run()
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
		metrics = append(metrics, m[i].metric)
	}
	return metrics
}

const (
	timeout       = time.Minute
	evalInterval  = time.Minute * 3
	retry         = time.Minute * 10
	stopThreshold = 5
)

func (e *metricsEngineImpl) Run() {
	e.register()
	go func() {
		timeoutCount := 0
		e.isRunning.Store(true)
		up.Set(1)
		defer func() {
			e.isRunning.Store(false)
			up.Set(0)
		}()
		for {
			batch := e.conn.NewBatch()
			for i := range e.metrics {
				batch.Queue(e.metrics[i].query)
			}

			batchCtx, cancelBatch := context.WithTimeout(e.ctx, timeout)
			results, err := e.conn.SendBatch(batchCtx, batch)
			if err != nil || batchCtx.Err() != nil {
				timeoutCount++
				log.Warn("msg", "error evaluating the batch", "err batch", err.Error(), "err batch context", batchCtx.Err())
			}
			handleResults(results, e.metrics)
			if err := results.Close(); err != nil {
				log.Warn("msg", "error closing batch", "err", err.Error())
			}

			cancelBatch() // Release resources if any.
			wait := evalInterval
			if timeoutCount > stopThreshold {
				// Stop the metrics engine temporarily if the batch times out beyond stopThreshold consecutively.
				log.Warn("msg", fmt.Sprintf("Database metric evaluation taking more time than expected. Stopping evaluation temporarily for %f minutes", retry.Minutes()))
				timeoutCount = 0
				wait = retry
				up.Set(0)
			}
			select {
			case <-e.ctx.Done():
				e.unregister()
				return
			case <-time.After(wait):
				up.Set(1)
			}
		}
	}()
}

func (e *metricsEngineImpl) IsRunning() bool {
	return e.isRunning.Load().(bool)
}

func handleResults(results pgx.BatchResults, m []metricQueryWrap) {
	for i := range m {
		metric := m[i]
		value := int64(0)
		err := results.QueryRow().Scan(&value)
		if err != nil {
			if metric.isHealthCheck {
				dbHealthErrors.Inc()
				updateMetric(metric.metric, 1)
			}
			log.Warn("msg", fmt.Sprintf("error evaluating database metric: %s", metric.metric), "err", err.Error())
			return
		}
		updateMetric(metric.metric, value)
	}
}

func updateMetric(m prometheus.Collector, value int64) {
	switch n := m.(type) {
	case prometheus.Gauge: // Keep Gauge above Counter, since Gauge satisfies Counter interface but not vice-versa.
		n.Set(float64(value))
	case prometheus.Counter:
		n.Add(float64(value))
	default:
		panic(fmt.Sprintf("metric %s is of type %T", m, m))
	}
}
