package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/timescale/promscale/pkg/api"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	//"github.com/influxdata/influxdb-observability/common"
	//"github.com/influxdata/influxdb-observability/jaeger-query-query-plugin/config"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

type connector struct {
	url string
	httpClient *http.Client
}

func NewReader(url string) spanstore.Reader {
	return &connector{
		httpClient: &http.Client{Timeout: time.Second*10},
	}
}

func (c *connector) GetServices(ctx context.Context) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, "post", c.url + api.JaegerQueryServicesEndpoint, nil)
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

func (c *connector) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var form url.Values
	form.Add("service_name", query.ServiceName)
	form.Add("span_kind", query.SpanKind)

	req, err := http.NewRequestWithContext(ctx, "post", c.url + api.JaegerQueryOperationsEndpoint, strings.NewReader(form.Encode()))
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

func (c *connector) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	var form url.Values
	form.Add("trace_id_low", fmt.Sprintf("%d", traceID.Low))
	form.Add("trace_id_high", fmt.Sprintf("%d", traceID.High))

	req, err := http.NewRequestWithContext(ctx, "post", c.url + api.JaegerQuerySingleTraceEndpoint, strings.NewReader(form.Encode()))
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

func (c *connector) FindTraces(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	return []*model.Trace{}, nil
}

func (c *connector) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	var traceIDs []model.TraceID
	return traceIDs, nil
}
