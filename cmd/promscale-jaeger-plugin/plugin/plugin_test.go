// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package plugin

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/api"
)

func TestGetServices(t *testing.T) {
	reader := newMockJaegerReader()
	s := httptest.NewServer(api.Services(new(api.Config), reader))
	defer s.Close()

	req, err := http.NewRequest("POST", s.URL, nil)
	require.NoError(t, err)

	client := http.Client{Timeout: time.Second}

	resp, err := client.Do(req)
	require.NoError(t, err)

	bSlice, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var response storage_v1.GetServicesResponse
	require.NoError(t, response.Unmarshal(bSlice))

	require.Equal(t, []string{"demo_service_1", "demo_service_2", "demo_service_3"}, response.GetServices())
}

func TestGetOperations(t *testing.T) {
	reader := newMockJaegerReader()
	s := httptest.NewServer(api.Operations(new(api.Config), reader))
	defer s.Close()

	r := storage_v1.GetOperationsRequest{
		Service:  "demo_service_1",
		SpanKind: "SPAN_KIND_SERVER",
	}

	bSlice, err := r.Marshal()
	require.NoError(t, err)

	req, err := http.NewRequest("POST", s.URL, bytes.NewReader(bSlice))
	require.NoError(t, err)

	client := http.Client{Timeout: time.Second}

	resp, err := client.Do(req)
	require.NoError(t, err)

	bSlice, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	var response storage_v1.GetOperationsResponse
	require.NoError(t, response.Unmarshal(bSlice))

	require.Equal(t, []string{"demo_operation_1", "demo_operation_2"}, response.GetOperationNames())
	require.Equal(t, 2, len(response.GetOperations()))
}
