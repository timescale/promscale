package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type Engine interface {
	Start()
	Stop()
}

type metricsEngineImpl struct {
	conn    pgxconn.PgxConn
	metrics []metricQueryWrap
	stop    chan struct{}
}

// NewEngine creates an engine that performs database metrics evaluation every evalInterval.
// Make sure to call this only when the database is TimescaleDB. Plain Postgres
// will cause evaluation errors.
func NewEngine(conn pgxconn.PgxConn) *metricsEngineImpl {
	return &metricsEngineImpl{
		conn:    conn,
		metrics: metrics,
		stop:    make(chan struct{}),
	}
}

func (e *metricsEngineImpl) register() {
	prometheus.MustRegister(getMetrics(e.metrics)...)
}

func (e *metricsEngineImpl) unregister() {
	prometheus.MustRegister(getMetrics(e.metrics)...)
}

func getMetrics(m []metricQueryWrap) []prometheus.Collector {
	var metrics []prometheus.Collector
	for i := range m {
		metrics = append(metrics, m[i].metric)
	}
	return metrics
}

const sqlMetricsEvalInterval = time.Minute * 3

func (e *metricsEngineImpl) Start() {
	e.register()
	go func() {
		evaluate := time.NewTicker(sqlMetricsEvalInterval)
		defer evaluate.Stop()
		numQueries := len(e.metrics)

		evalContext, stopEvaluation := context.WithCancel(context.Background())
		for {
			// Run for once during start.
			wg := new(sync.WaitGroup)
			wg.Add(numQueries)
			for i := range e.metrics {
				go evalMetric(evalContext, e.conn, wg, &e.metrics[i])
			}
			wg.Wait()
			select {
			case <-e.stop:
				stopEvaluation()
				return
			case <-evaluate.C:
			}
		}
	}()
}

func evalMetric(ctx context.Context, conn pgxconn.PgxConn, wg *sync.WaitGroup, metric *metricQueryWrap) {
	defer func() {
		wg.Done()
	}()
	var value int64
	err := conn.QueryRow(ctx, metric.query).Scan(&value)
	if err != nil {
		log.Error("msg", fmt.Sprintf("error evaluating database metric: %s", metric.metric), "err", err.Error())
		return
	}
	if metric.isCounter {
		// Since counter needs the amount incremented only.
		value = value - metric.previousVal
		metric.previousVal += value
	}
	updateMetric(metric.metric, value)
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

func (e *metricsEngineImpl) Stop() {
	close(e.stop)
	e.unregister()
}
