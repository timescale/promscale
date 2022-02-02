// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

type Query struct {
	conn pgxconn.PgxConn
}

func New(conn pgxconn.PgxConn, t telemetry.Engine) (*Query, error) {
	if err := registerMetricsForTelemetry(t); err != nil {
		return nil, fmt.Errorf("register metrics for telemetry: %w", err)
	}
	return &Query{conn}, nil
}

func (p *Query) SpanReader() spanstore.Reader {
	return p
}

func (p *Query) DependencyReader() dependencystore.Reader {
	return p
}

func (p *Query) SpanWriter() spanstore.Writer {
	panic("Use Promscale + OTEL-collector to ingest traces")
}

func (p *Query) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_trace", "code": ""}).Inc()
	start := time.Now()
	res, err := getTrace(ctx, p.conn, traceID)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_trace", "code": "200"}).Inc()
		traceRequestsExec.Add(1)
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "get_trace"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_trace", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func (p *Query) GetServices(ctx context.Context) ([]string, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_services", "code": ""}).Inc()
	start := time.Now()
	res, err := getServices(ctx, p.conn)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_services", "code": "200"}).Inc()
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "get_services"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_services", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func (p *Query) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_operations", "code": ""}).Inc()
	start := time.Now()
	res, err := getOperations(ctx, p.conn, query)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_operations", "code": "200"}).Inc()
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "get_operations"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_operations", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func (p *Query) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_traces", "code": ""}).Inc()
	start := time.Now()
	res, err := findTraces(ctx, p.conn, query)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_traces", "code": "200"}).Inc()
		traceRequestsExec.Add(1)
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "find_traces"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_traces", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func (p *Query) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_trace_ids", "code": ""}).Inc()
	start := time.Now()
	res, err := findTraceIDs(ctx, p.conn, query)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_trace_ids", "code": "200"}).Inc()
		traceRequestsExec.Add(1)
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "find_trace_ids"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "find_trace_ids", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func (p *Query) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_dependencies", "code": ""}).Inc()
	start := time.Now()
	res, err := getDependencies(ctx, p.conn, endTs, lookback)
	if err == nil {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_dependencies", "code": "200"}).Inc()
		dependencyRequestsExec.Add(1)
		metrics.RequestsDuration.With(prometheus.Labels{"subsystem": "trace", "handler": "get_dependencies"}).Observe(time.Since(start).Seconds())
	} else {
		metrics.RequestsTotal.With(prometheus.Labels{"subsystem": "trace", "handler": "get_dependencies", "code": "500"}).Inc()
	}
	return res, logError(err)
}

func logError(err error) error {
	if err != nil {
		log.Error("msg", "Error in jaeger query GRPC response", "err", err)
	}
	return err
}
