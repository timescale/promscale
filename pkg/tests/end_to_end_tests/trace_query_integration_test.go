// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/model/otlpgrpc"
	"google.golang.org/grpc"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	jaegerquery "github.com/timescale/promscale/pkg/jaeger/query"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"

	jaegerQueryApp "github.com/jaegertracing/jaeger/cmd/query/app"
	jaegerQueryService "github.com/jaegertracing/jaeger/cmd/query/app/querysvc"
	jaegerJSONModel "github.com/jaegertracing/jaeger/model/json"
)

func TestCompareTraceQueryResponse(t *testing.T) {
	// Start containers.
	jaegerContainer, _, jaegerIP, jaegerReceivingPort, _, uiPort, err := testhelpers.StartJaegerContainer(false)
	require.NoError(t, err)
	defer jaegerContainer.Terminate(context.Background())

	otelContainer, otelHost, otelReceivingPort, err := testhelpers.StartOtelCollectorContainer(fmt.Sprintf("%s:%s", jaegerIP, jaegerReceivingPort.Port()), false)
	require.NoError(t, err)
	defer otelContainer.Terminate(context.Background())

	// Make clients.
	otelIngestClient, err := getOtelIngestClient(fmt.Sprintf("%s:%s", otelHost, otelReceivingPort.Port()))
	require.NoError(t, err)

	sampleTraces := generateTestTrace()

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)

		// Ingest traces into Promscale.
		err = ingestor.IngestTraces(context.Background(), copyTraces(sampleTraces))
		require.NoError(t, err)

		// Ingest traces into otel-collector -> Jaeger.
		resp, err := otelIngestClient.Export(context.Background(), newTracesRequest(copyTraces(sampleTraces)))
		require.NoError(t, err)
		require.NotNil(t, resp)

		//time.Sleep(time.Hour)

		// Create Promscale proxy to query traces data.
		promscaleProxy := jaegerquery.New(pgxconn.NewPgxConn(db))

		wg := new(sync.WaitGroup)
		promscaleJSONWrapper := getPromscaleTraceQueryJSONServer(promscaleProxy, wg, ":9203")
		wg.Add(1)
		// Run the JSON wrapper server.
		go promscaleJSONWrapper()
		// Wait for JSON wrapper to be up.
		wg.Wait()

		promscaleClient := httpClient{"http://localhost:9203"}
		jaegerClient := httpClient{"http://localhost:" + uiPort.Port()}

		// Verify services.
		promscaleResp, err := promscaleClient.getServices()
		require.NoError(t, err)

		jaegerResp, err := jaegerClient.getServices()
		require.NoError(t, err)

		require.Equal(t, jaegerResp, promscaleResp)

		// Verify Operations.
		promscaleResp, err = promscaleClient.getOperations("service1")
		require.NoError(t, err)

		fmt.Println("operation p", promscaleResp)

		jaegerResp, err = jaegerClient.getOperations("service1")
		require.NoError(t, err)
		fmt.Println("operation j", jaegerResp)

		require.Equal(t, jaegerResp, promscaleResp)

		// Verify a single trace fetch.
		promscaleResp, err = promscaleClient.getTrace("31323334353637383930313233343536")
		require.NoError(t, err)

		fmt.Println("trace p", promscaleResp)

		jaegerResp, err = jaegerClient.getTrace("31323334353637383930313233343536")
		require.NoError(t, err)
		fmt.Println("trace j", jaegerResp)

		require.Equal(t, jaegerResp, promscaleResp)
	})
}

// getPromscaleTraceQueryJSONServer returns a wrapper around promscale trace query module, that responds into
// Jaeger JSON responses. It reuses Jaeger's JSON layer as a wrapper so that the outputs from
// Jaeger's `/api` endpoints can be directly compared with Promscale's output from query endpoints.
func getPromscaleTraceQueryJSONServer(proxy *jaegerquery.Query, wg *sync.WaitGroup, port string) (concurrentServer func()) {
	promscaleQueryHandler := jaegerQueryApp.NewAPIHandler(jaegerQueryService.NewQueryService(
		proxy,
		proxy,
		jaegerQueryService.QueryServiceOptions{}))

	r := jaegerQueryApp.NewRouter()
	promscaleQueryHandler.RegisterRoutes(r)

	server := http.Server{Handler: r}

	return func() {
		listener, err := net.Listen("tcp", port)
		if err != nil {
			panic(err)
		}

		go func() {
			// Wait for a second for server to serve and then mark done.
			time.Sleep(time.Second)
			wg.Done()
		}()
		if err := server.Serve(listener); err != nil {
			panic(err)
		}
	}
}

func getOtelIngestClient(url string) (client otlpgrpc.TracesClient, err error) {
	grpcConn, err := grpc.DialContext(context.Background(), url, []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}...)
	if err != nil {
		return nil, fmt.Errorf("dial with context: %w", err)
	}
	client = otlpgrpc.NewTracesClient(grpcConn)
	return
}

// From jaeger. https://github.com/jaegertracing/jaeger/blob/6a35ad526c2dad575513f2d6a9c87899f9d733d8/cmd/query/app/http_handler.go#L63
type structuredResponse struct {
	Data   interface{}       `json:"data"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
	Errors []structuredError `json:"errors"`
}

type structuredError struct {
	Code    int                     `json:"code,omitempty"`
	Msg     string                  `json:"msg"`
	TraceID jaegerJSONModel.TraceID `json:"traceID,omitempty"`
}

type httpClient struct {
	url string
}

const (
	servicesEndpoint    = "%s/api/services"
	operationsEndpoint  = "%s/api/services/%s/operations"
	singleTraceEndpoint = "%s/api/traces/%s"
)

func (c httpClient) getServices() (*structuredResponse, error) {
	resp, err := do(fmt.Sprintf(servicesEndpoint, c.url))
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	return resp, nil
}

func (c httpClient) getOperations(service string) (*structuredResponse, error) {
	resp, err := do(fmt.Sprintf(operationsEndpoint, c.url, service))
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	return resp, nil
}

func (c httpClient) getTrace(traceId string) (*structuredResponse, error) {
	fmt.Println("url", fmt.Sprintf(singleTraceEndpoint, c.url, traceId))
	resp, err := do(fmt.Sprintf(singleTraceEndpoint, c.url, traceId))
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	return resp, nil
}

func do(url string) (*structuredResponse, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read all: %w", err)
	}
	var r structuredResponse
	if err = json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &r, nil
}
