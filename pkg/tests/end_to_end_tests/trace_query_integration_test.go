// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.opentelemetry.io/collector/model/otlpgrpc"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	jaegerquery "github.com/timescale/promscale/pkg/jaeger/query"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestCompareTraceQueryResponse(t *testing.T) {
	// Start containers.
	jaegerContainer, jaegerHost, jaegerIP, jaegerReceivingPort, jaegerQueryingPort, _, err := testhelpers.StartJaegerContainer(false)
	require.NoError(t, err)
	defer jaegerContainer.Terminate(context.Background())

	otelContainer, otelHost, otelReceivingPort, err := testhelpers.StartOtelCollectorContainer(fmt.Sprintf("%s:%s", jaegerIP, jaegerReceivingPort.Port()), false)
	require.NoError(t, err)
	defer otelContainer.Terminate(context.Background())

	// Make clients.
	jaegerQueryClient, err := getJaegerQueryClient(fmt.Sprintf("%s:%s", jaegerHost, jaegerQueryingPort.Port()))
	require.NoError(t, err)

	otelIngestClient, err := getOtelIngestClient(fmt.Sprintf("%s:%s", otelHost, otelReceivingPort.Port()))
	require.NoError(t, err)

	sampleTraces := generateTestTrace()

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)

		err = ingestor.IngestTraces(context.Background(), copyTraces(sampleTraces))
		require.NoError(t, err)

		resp, err := otelIngestClient.Export(context.Background(), newTracesRequest(copyTraces(sampleTraces)))
		require.NoError(t, err)
		require.NotNil(t, resp)

		promscaleProxy := jaegerquery.New(pgxconn.NewPgxConn(db))
		time.Sleep(time.Hour)

		// Compare services.
		require.True(t, matchServices(t, jaegerQueryClient, promscaleProxy))
	})
}

func getJaegerQueryClient(url string) (client api_v2.QueryServiceClient, err error) {
	grpcConn, err := grpc.DialContext(context.Background(), url, []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}...)
	if err != nil {
		return nil, fmt.Errorf("dial with context: %w", err)
	}
	client = api_v2.NewQueryServiceClient(grpcConn)
	return
}

func getOtelIngestClient(url string) (client otlpgrpc.TracesClient, err error) {
	grpcConn, err := grpc.DialContext(context.Background(), url, []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()}...)
	if err != nil {
		return nil, fmt.Errorf("dial with context: %w", err)
	}
	client = otlpgrpc.NewTracesClient(grpcConn)
	return
}

func matchServices(t testing.TB, jaegerClient api_v2.QueryServiceClient, promscaleClient *jaegerquery.Query) bool {
	jResp, err := jaegerClient.GetServices(context.Background(), &api_v2.GetServicesRequest{})
	require.NoError(t, err)
	jServices := jResp.GetServices()

	pServices, err := promscaleClient.GetServices(context.Background())
	require.NoError(t, err)

	sort.Strings(jServices)
	sort.Strings(pServices)

	fmt.Println("jservices", jServices)
	fmt.Println("pservices", pServices)

	return reflect.DeepEqual(jServices, pServices)
}
