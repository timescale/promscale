package plugin

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/timescale/promscale/pkg/api"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type Plugin struct {
	name       string
	url        string
	httpClient *http.Client

	logger hclog.Logger
}

func New(url string, logger hclog.Logger, timeout time.Duration) *Plugin {
	return &Plugin{
		url:        url,
		httpClient: &http.Client{Timeout: timeout},
		logger:     logger,
	}
}

func (p *Plugin) SpanReader() spanstore.Reader {
	return p
}

func (p *Plugin) DependencyReader() dependencystore.Reader {
	return p
}

func (p *Plugin) SpanWriter() spanstore.Writer {
	panic("Use Promscale + OTEL-collector to ingest traces")
}

func (p *Plugin) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	request := &storage_v1.GetTraceRequest{
		TraceID: traceID,
	}
	bSlice, err := p.waitForResponse(ctx, request, api.JaegerQuerySingleTraceEndpoint)
	if err != nil {
		return nil, wrapErr(api.JaegerQuerySingleTraceEndpoint, fmt.Errorf("wait for response: %w", err))
	}

	var response model.Trace
	if err = response.Unmarshal(bSlice); err != nil {
		return nil, wrapErr(api.JaegerQuerySingleTraceEndpoint, fmt.Errorf("unmarhshal response: %w", err))
	}

	return &response, nil
}

func (p *Plugin) GetServices(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", p.url+api.JaegerQueryServicesEndpoint, nil)
	if err != nil {
		return nil, wrapErr(api.JaegerQueryServicesEndpoint, fmt.Errorf("creating request: %w", err))
	}
	applyValidityHeaders(req)

	// todo: make this compatible in waitForResponse()
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, wrapErr(api.JaegerQueryServicesEndpoint, fmt.Errorf("fetching response: %w", err))
	}
	if err = validateResponse(resp); err != nil {
		return nil, wrapErr(api.JaegerQueryServicesEndpoint, fmt.Errorf("validate response: %w", err))
	}

	bSlice, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, wrapErr(api.JaegerQueryServicesEndpoint, fmt.Errorf("reading response body: %w", err))
	}

	var response storage_v1.GetServicesResponse
	if err = response.Unmarshal(bSlice); err != nil {
		return nil, wrapErr(api.JaegerQueryServicesEndpoint, fmt.Errorf("unmarshalling response: %w", err))
	}
	return response.GetServices(), nil
}

type marshallable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

func (p *Plugin) waitForResponse(ctx context.Context, requestData marshallable, api string) (responseData []byte, err error) {
	reqSlice, err := requestData.Marshal()
	if err != nil {
		return nil, wrapErr(api, fmt.Errorf("marshalling request: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, "POST", p.url+api, bytes.NewReader(reqSlice))
	if err != nil {
		return nil, wrapErr(api, fmt.Errorf("creating request: %w", err))
	}
	applyValidityHeaders(req)

	response, err := p.httpClient.Do(req)
	if err != nil {
		return nil, wrapErr(api, fmt.Errorf("fetching response: %w", err))
	}
	if err = validateResponse(response); err != nil {
		return nil, wrapErr(api, fmt.Errorf("validate response: %w", err))
	}

	bSlice, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, wrapErr(api, fmt.Errorf("reading response body: %w", err))
	}
	return bSlice, nil
}

func (p *Plugin) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	p.logger.Warn("msg", "into operations")
	request := &storage_v1.GetOperationsRequest{
		Service:  query.ServiceName,
		SpanKind: query.SpanKind,
	}
	bSlice, err := p.waitForResponse(ctx, request, api.JaegerQueryOperationsEndpoint)
	if err != nil {
		return nil, wrapErr(api.JaegerQueryOperationsEndpoint, fmt.Errorf("wait for response: %w", err))
	}

	var resp storage_v1.GetOperationsResponse
	if err = resp.Unmarshal(bSlice); err != nil {
		return nil, wrapErr(api.JaegerQueryOperationsEndpoint, fmt.Errorf("unmarshalling response: %w", err))
	}
	var operations []spanstore.Operation
	if resp.Operations != nil {
		for _, operation := range resp.Operations {
			operations = append(operations, spanstore.Operation{
				Name:     operation.Name,
				SpanKind: operation.SpanKind,
			})
		}
	}
	return operations, nil
}

func (p *Plugin) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	request := &storage_v1.FindTracesRequest{
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
	response, err := p.waitForResponse(ctx, request, api.JaegerQueryTracesEndpoint)
	if err != nil {
		return nil, wrapErr(api.JaegerQueryTracesEndpoint, fmt.Errorf("wait for response: %w", err))
	}

	var resp storage_v1.SpansResponseChunk
	if err = resp.Unmarshal(response); err != nil {
		return nil, wrapErr(api.JaegerQueryTracesEndpoint, fmt.Errorf("unmarshalling response: %w", err))
	}

	// Copied from Jaeger's grpc_client.go
	// https://github.com/jaegertracing/jaeger/blob/067dff713ab635ade66315bbd05518d7b28f40c6/plugin/storage/grpc/shared/grpc_client.go#L179
	var traces []*model.Trace
	var trace *model.Trace
	var traceID model.TraceID
	for i, span := range resp.Spans {
		if span.TraceID != traceID {
			trace = &model.Trace{}
			traceID = span.TraceID
			traces = append(traces, trace)
		}
		trace.Spans = append(trace.Spans, &resp.Spans[i])
	}
	return nil, nil
}

func (p *Plugin) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	return nil, nil
}

func (p *Plugin) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	p.logger.Warn("msg", "GetDependencies is yet to be implemented")
	return nil, nil
}
