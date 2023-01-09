// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type Store struct {
	conn     pgxconn.PgxConn
	inserter ingestor.DBInserter
	builder  *Builder
}

func New(conn pgxconn.PgxConn, inserter ingestor.DBInserter, cfg *Config) *Store {
	return &Store{conn, inserter, NewBuilder(cfg)}
}

func (p *Store) SpanReader() spanstore.Reader {
	return p
}

func (p *Store) DependencyReader() dependencystore.Reader {
	return p
}

func (p *Store) SpanWriter() spanstore.Writer {
	return p
}

func (p *Store) StreamingSpanWriter() spanstore.Writer {
	return p
}

func (p *Store) WriteSpan(ctx context.Context, span *model.Span) error {
	traces, err := ProtoToTraces(span)
	if err != nil {
		return err
	}
	return p.inserter.IngestTraces(ctx, traces)
}

// Close performs graceful shutdown of SpanWriter on Jaeger collector shutdown.
// In our case we have nothing to do
// Noop impl avoid getting GRPC error message when Jaeger collector shuts down.
func (p *Store) Close() error {
	return nil
}

func (p *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Get_Trace", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()
	res, err := getTrace(ctx, p.builder, p.conn, traceID)

	if err != nil {
		if !errors.Is(err, spanstore.ErrTraceNotFound) {
			err = logError(err)
		}
		return nil, err
	}

	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Store) GetServices(ctx context.Context) ([]string, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Get_Services", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()
	res, err := getServices(ctx, p.conn)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	return res, nil
}

func (p *Store) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Get_Operations", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()
	res, err := getOperations(ctx, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	return res, nil
}

func (p *Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Find_Traces", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()
	res, err := findTraces(ctx, p.builder, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Find_Trace_IDs", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()
	res, err := findTraceIDs(ctx, p.builder, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		labels := prometheus.Labels{"type": "trace", "handler": "Get_Dependencies", "code": code, "reason": ""}
		metrics.Query.With(labels).Inc()
		delete(labels, "reason")
		metrics.QueryDuration.With(labels).Observe(time.Since(start).Seconds())
	}()

	res, err := getDependencies(ctx, p.conn, endTs, lookback)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	dependencyRequestsExec.Add(1)
	return res, nil
}

func (p *Store) GetBuilder() *Builder {
	return p.builder
}

func logError(err error) error {
	if err != nil {
		log.Error("msg", "Error in jaeger query GRPC response", "err", err)
	}
	return err
}
