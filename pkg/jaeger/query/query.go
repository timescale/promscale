// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"fmt"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

type Query struct {
	conn pgxconn.PgxConn
}

func New(conn pgxconn.PgxConn, t telemetry.Telemetry) (*Query, error) {
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
	res, err := getTrace(ctx, p.conn, traceID)
	if err == nil {
		traceRequestsExec.Add(1)
	}
	return res, logError(err)
}

func (p *Query) GetServices(ctx context.Context) ([]string, error) {
	res, err := getServices(ctx, p.conn)
	return res, logError(err)
}

func (p *Query) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	res, err := getOperations(ctx, p.conn, query)
	return res, logError(err)
}

func (p *Query) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	start := time.Now()
	res, err := findTraces(ctx, p.conn, query)
	if err == nil {
		traceExecutionTime.Observe(time.Since(start).Seconds())
		traceRequestsExec.Add(1)
	}
	return res, logError(err)
}

func (p *Query) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	res, err := findTraceIDs(ctx, p.conn, query)
	if err == nil {
		traceRequestsExec.Add(1)
	}
	return res, logError(err)
}

func (p *Query) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	res, err := getDependencies(ctx, p.conn, endTs, lookback)
	if err == nil {
		dependencyRequestsExec.Add(1)
	}
	return res, logError(err)
}

func logError(err error) error {
	if err != nil {
		log.Error("msg", "Error in jaeger query GRPC response", "err", err)
	}
	return err
}
