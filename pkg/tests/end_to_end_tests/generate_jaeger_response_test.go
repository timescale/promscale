// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
//go:build gen_response
// +build gen_response

// Below line generates the Jaeger API responses.
//go:generate go test . -run TestGenerateJaegerAPIResponses

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	jaegerproto "github.com/jaegertracing/jaeger/proto-gen/api_v2"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
)

// TestGenerateJaegerAPIResponses is not an actual test, rather a function to spin up jaeger container
// and store the responses based on the test cases, so that they can be statically compared in the
// order of the testcases.
//
// We needed to generate and save these responses (as opposed to re-running this live during a test) to
// save work during CI runs and prevent hitting resource limits.
func TestGenerateJaegerAPIResponses(t *testing.T) {
	jaeger, err := testhelpers.StartJaegerContainer(true)
	require.NoError(t, err)

	sampleTraces := generateTestTrace()
	err = insertDataIntoJaeger(fmt.Sprintf("localhost:%s", jaeger.GrpcReceivingPort.Port()), copyTraces(sampleTraces))
	require.NoError(t, err)

	var store traceResponsesStore
	client := httpClient{url: "http://localhost:" + jaeger.UIPort.Port()}
	store.Responses = make([]traceResponse, 0)

	queries := traceQueryCases
	for i := range queries {
		tc := queries[i]
		services := getServices(t, client)
		operations := getOperations(t, client, tc.service)
		trace := getTrace(t, client, tc.traceID)
		traces := getTraces(t, client, tc.service, tc.start, tc.end, tc.tag)
		store.Responses = append(store.Responses, traceResponse{
			Services:   services,
			Operations: operations,
			Trace:      trace,
			Traces:     traces,
		})
	}

	require.NoError(t, storeJaegerQueryResponses(&store))
}

func insertDataIntoJaeger(endpoint string, data ptrace.Traces) error {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		panic(err)
	}

	client := jaegerproto.NewCollectorServiceClient(conn)

	batches, err := jaegertranslator.ProtoFromTraces(data)
	if err != nil {
		panic(err)
	}

	for _, batch := range batches {
		_, err := client.PostSpans(context.Background(), &jaegerproto.PostSpansRequest{Batch: *batch}, grpc.WaitForReady(true))
		if err != nil {
			panic(err)
		}
	}
	return nil
}
