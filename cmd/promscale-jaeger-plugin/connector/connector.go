package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/timescale/promscale/pkg/api"
)

var (
	_ shared.StoragePlugin   = (*Connector)(nil)
	_ spanstore.Reader       = (*Connector)(nil)
	_ dependencystore.Reader = (*Connector)(nil)
)

type Connector struct {
	url        string
	httpClient *http.Client
}

func NewReader(url string) *Connector {
	return &Connector{
		url:        url,
		httpClient: &http.Client{Timeout: time.Second * 10},
	}
}

func (c *Connector) SpanWriter() spanstore.Writer {
	panic("Use Promscale coupled with OTLP-collector to ingest data")
}

func (c *Connector) SpanReader() spanstore.Reader {
	return c
}

func (c *Connector) DependencyReader() dependencystore.Reader {
	return nil
}

func (c *Connector) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	return nil, nil
}

func (c *Connector) GetServices(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.url+api.JaegerQueryServicesEndpoint, bytes.NewReader([]byte("temp")))
	if err != nil {
		return nil, fmt.Errorf("creating request from Promscale: %w", err)
	}
	applyValidityHeaders(req)

	// todo: reduce the below code's redundancy.
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from Promscale: %w", err)
	}

	bSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from Promscale: %w", err)
	}

	var services []string
	if err = json.Unmarshal(bSlice, &services); err != nil {
		return nil, fmt.Errorf("unmarshalling response from Promscale: %w", err)
	}
	return services, nil
}

func (c *Connector) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var form url.Values
	form.Add("service_name", query.ServiceName)
	form.Add("span_kind", query.SpanKind)

	req, err := http.NewRequestWithContext(ctx, "post", c.url+api.JaegerQueryOperationsEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating request from Promscale: %w", err)
	}
	applyValidityHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from Promscale: %w", err)
	}

	bSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from Promscale: %w", err)
	}

	var operations []spanstore.Operation
	if err = json.Unmarshal(bSlice, &operations); err != nil {
		return nil, fmt.Errorf("unmarshalling response from Promscale: %w", err)
	}
	return operations, nil
}

func (c *Connector) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	var form url.Values
	form.Add("trace_id_low", fmt.Sprintf("%d", traceID.Low))
	form.Add("trace_id_high", fmt.Sprintf("%d", traceID.High))

	req, err := http.NewRequestWithContext(ctx, "post", c.url+api.JaegerQuerySingleTraceEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating request from Promscale: %w", err)
	}
	applyValidityHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from Promscale: %w", err)
	}

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from Promscale: %w", err)
	}

	return nil, nil
}

func (c *Connector) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	r := &storage_v1.FindTracesRequest{
		Query: &storage_v1.TraceQueryParameters{
			ServiceName:   query.ServiceName,
			OperationName: query.OperationName,
			Tags:          query.Tags,
			StartTimeMin:  query.StartTimeMin,
			StartTimeMax:  query.StartTimeMax,
			DurationMin:   query.DurationMin,
			DurationMax:   query.DurationMax,
			NumTraces:     int32(query.NumTraces),
		},
	}
	// todo: next version improvement: snappy compress.
	bSlice, err := r.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshalling 'findTracesRequest': %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "post", c.url+api.JaegerQuerySingleTraceEndpoint, bytes.NewReader(bSlice))
	if err != nil {
		return nil, fmt.Errorf("creating request from Promscale: %w", err)
	}
	applyValidityHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching response from Promscale: %w", err)
	}

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body from Promscale: %w", err)
	}

	return []*model.Trace{}, nil
}

func (c *Connector) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	var traceIDs []model.TraceID
	return traceIDs, nil
}
