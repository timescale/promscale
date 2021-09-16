package store

import (
	"context"
	"fmt"
	"net/http"
	"time"

	//"github.com/influxdata/influxdb-observability/common"
	//"github.com/influxdata/influxdb-observability/jaeger-query-plugin/config"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	_ shared.StoragePlugin   = (*Store)(nil)
	_ spanstore.Reader       = (*Store)(nil)
	_ dependencystore.Reader = (*Store)(nil)
)

const (
	measurementSpans     = "spans"
	measurementLogs      = "logs"
	measurementSpanLinks = "span-links"

	// These attribute key names are influenced by the proto message keys.
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/trace/v1/trace.proto
	attributeTime             = "time"
	attributeTraceID          = "trace_id"
	attributeSpanID           = "span_id"
	attributeParentSpanID     = "parent_span_id"
	attributeName             = "name"
	attributeBody             = "body"
	attributeSpanKind         = "kind"
	attributeEndTimeUnixNano  = "end_time_unix_nano"
	attributeDurationNano     = "duration_nano"
	attributeStatusCode       = "otel.status_code"
	attributeStatusCodeError  = "ERROR"
	attributeLinkedTraceID    = "linked_trace_id"
	attributeLinkedSpanID     = "linked_span_id"
	attributeServiceName      = "service.name"
	attributeTelemetrySDKName = "telemetry.sdk.name"
)

type Store struct {
	//host     string
	//database string

	httpClient *http.Client
}

func NewStore() (*Store, error) {
	return &Store{
		//host:       conf.Host,
		//database:   conf.Database,
		httpClient: &http.Client{Timeout: time.Second*10},
	}, nil
}

func (s *Store) SpanReader() spanstore.Reader {
	return s
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return s
}

func (s *Store) SpanWriter() spanstore.Writer {
	panic("writer not implemented, use the InfluxDB OpenTelemetry Collector exporter")
}

func (s *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	// Get spans
	fmt.Println("came into get-trace")
	return nil, nil
}

func (s *Store) GetServices(ctx context.Context) ([]string, error) {
	var services []string
	http.Get("http://localhost:9201/api/v1/report")
	return services, nil
}

func (s *Store) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation
	return operations, nil
}

func (s *Store) FindTraces(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	return []*model.Trace{}, nil
}

func (s *Store) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	var traceIDs []model.TraceID
	return traceIDs, nil
}

func (s *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	var dependencyLinks []model.DependencyLink
	return dependencyLinks, nil
}