// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type Query struct {
	conn pgxconn.PgxConn
}

func New(conn pgxconn.PgxConn) *Query {
	return &Query{conn}
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
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Get_Trace", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Get_Trace", "code": code}).Observe(time.Since(start).Seconds())
	}()
	res, err := getTrace(ctx, p.conn, traceID)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Query) GetServices(ctx context.Context) ([]string, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Get_Services", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Get_Services", "code": code}).Observe(time.Since(start).Seconds())
	}()
	res, err := getServices(ctx, p.conn)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	return res, nil
}

func (p *Query) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Get_Operations", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Get_Operations", "code": code}).Observe(time.Since(start).Seconds())
	}()
	res, err := getOperations(ctx, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	return res, nil
}

func (p *Query) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Find_Traces", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Find_Traces", "code": code}).Observe(time.Since(start).Seconds())
	}()
	res, err := findTraces(ctx, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Query) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Find_Trace_IDs", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Find_Trace_IDs", "code": code}).Observe(time.Since(start).Seconds())
	}()
	res, err := findTraceIDs(ctx, p.conn, query)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	traceRequestsExec.Add(1)
	return res, nil
}

func (p *Query) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	code := "5xx"
	start := time.Now()
	defer func() {
		metrics.Query.With(prometheus.Labels{"type": "trace", "handler": "Get_Dependencies", "code": code}).Inc()
		metrics.QueryDuration.With(prometheus.Labels{"type": "trace", "handler": "Get_Dependencies", "code": code}).Observe(time.Since(start).Seconds())
	}()

	res, err := getDependencies(ctx, p.conn, endTs, lookback)
	if err != nil {
		return nil, logError(err)
	}
	code = "2xx"
	dependencyRequestsExec.Add(1)
	return res, nil
}

func logError(err error) error {
	if err != nil {
		log.Error("msg", "Error in jaeger query GRPC response", "err", err)
	}
	return err
}
