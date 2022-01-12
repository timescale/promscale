// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"
	jaegerquery "github.com/timescale/promscale/pkg/jaeger/query"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"

	jaegerQueryApp "github.com/jaegertracing/jaeger/cmd/query/app"
	jaegerQueryService "github.com/jaegertracing/jaeger/cmd/query/app/querysvc"
	jaegerJSONModel "github.com/jaegertracing/jaeger/model/json"
)

type traceQuery struct {
	name    string
	start   int64
	end     int64
	service string
	traceID [16]byte // For testing getTrace API.
}

// To generate Jaeger responses, run the TestGenerateJaegerAPIResponses test function
// which iterates through these query cases, spins up Jaeger container and store the
// performed query cases as per their order in the file.
//
// To use the generated responses, call loadJaegerQueryResponses() which returns the
// stored data in the file.
var traceQueryCases = []traceQuery{
	{
		name:    "simple trace",
		start:   timestamp.FromTime(testSpanStartTime) * 1000,
		end:     timestamp.FromTime(testSpanEndTime) * 1000,
		service: service0,
		traceID: traceID1,
	},
}

func TestCompareTraceQueryResponse(t *testing.T) {
	sampleTraces := generateTestTrace()
	e2eDb := *testDatabase + "_e2e"

	withDB(t, e2eDb, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		// Ingest traces into Promscale.
		err = ingestor.IngestTraces(context.Background(), copyTraces(sampleTraces))
		require.NoError(t, err)

		// Create Promscale proxy to query traces data.
		promscaleProxy, err := jaegerquery.New(pgxconn.NewPgxConn(db), telemetry.NewNoopEngine())
		require.NoError(t, err)

		shutdown := runPromscaleTraceQueryJSONServer(promscaleProxy, ":9203")
		defer shutdown.Close()

		promscaleClient := httpClient{"http://localhost:9203"}

		jaegerResponse, err := loadJaegerQueryResponses()
		require.NoError(t, err)

		for i, tc := range traceQueryCases {
			tcJaegerResponse := jaegerResponse.Responses[i]

			// Verify services.
			promscaleServices := getServices(t, promscaleClient)
			require.Equal(t, tcJaegerResponse.Services, promscaleServices, tc.name)

			// Verify Operations.
			promscaleOps := getOperations(t, promscaleClient, tc.service)
			require.Equal(t, tcJaegerResponse.Operations, promscaleOps, tc.name)

			// Verify a single trace fetch.
			promscaleTrace := getTrace(t, promscaleClient, tc.traceID)
			require.Exactly(t, tcJaegerResponse.Trace, promscaleTrace, tc.name)

			// Verify fetch traces API.
			promscaleTraces := getTraces(t, promscaleClient, tc.service, tc.start, tc.end)
			require.Exactly(t, tcJaegerResponse.Traces, promscaleTraces, tc.name)
		}
	})
}

var skipTags = map[string]struct{}{
	"internal.span.format": {},
	"otel.status_code":     {},
}

func sortTraceContents(t *jaegerJSONModel.Trace) {
	sort.Strings(t.Warnings)

	sortSpanContents(t.Spans, skipTags)
}

func sortSpanContents(spans []jaegerJSONModel.Span, ignoreTags map[string]struct{}) {
	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].SpanID < spans[j].SpanID
	})

	for index := range spans {
		// Sort references.
		sort.SliceStable(spans[index].References, func(i, j int) bool {
			return spans[index].References[i].SpanID < spans[index].References[j].SpanID
		})

		spans[index].Tags = sortFields(trimIgnoredTags(spans[index].Tags[:], ignoreTags))

		// Sort warnings.
		sort.Strings(spans[index].Warnings)

		if spans[index].Process != nil {
			// Sort processes tags.
			spans[index].Process.Tags = sortFields(spans[index].Process.Tags[:])
		}

		// Sort tags in each log.
		for logIndex := range spans[index].Logs {
			// Sort tags belonging to a log.
			spans[index].Logs[logIndex].Fields = sortFields(spans[index].Logs[logIndex].Fields[:])
		}
	}
}

func sortFields(f []jaegerJSONModel.KeyValue) []jaegerJSONModel.KeyValue {
	sort.Slice(f, func(i, j int) bool {
		v1 := fmt.Sprintf("%s%v", f[i].Key, f[i].Value)
		v2 := fmt.Sprintf("%s%v", f[j].Key, f[j].Value)
		return v1 < v2
	})
	return f
}

func purgeProcessInformation(a *jaegerJSONModel.Trace) {
	a.Processes = nil
	for i := range a.Spans {
		a.Spans[i].ProcessID = ""
	}
}

func trimIgnoredTags(tags []jaegerJSONModel.KeyValue, ignoreTags map[string]struct{}) (trimmedTags []jaegerJSONModel.KeyValue) {
	trimmedTags = make([]jaegerJSONModel.KeyValue, 0)
	for _, tag := range tags {
		if _, ignore := ignoreTags[tag.Key]; !ignore {
			trimmedTags = append(trimmedTags, tag)
		}
	}
	return
}

func convertToStringArr(arr []interface{}) []string {
	// API: Services, Operations.
	bSlice, err := json.Marshal(arr)
	if err != nil {
		panic("err while re-marshaling 'Data' for map type conversion: " + err.Error())
	}
	tmp := make([]string, 0)
	if err = json.Unmarshal(bSlice, &tmp); err != nil {
		panic(err)
	}
	return tmp
}

func convertToTrace(i interface{}) jaegerJSONModel.Trace {
	// API: Get Trace.
	bSlice, err := json.Marshal(i)
	if err != nil {
		panic("err while re-marshaling 'Data' for map type conversion: " + err.Error())
	}
	var tmp jaegerJSONModel.Trace
	if err = json.Unmarshal(bSlice, &tmp); err != nil {
		panic(err)
	}
	return tmp
}

func convertToTraces(arr []interface{}) []jaegerJSONModel.Trace {
	// API: Fetch traces.
	bSlice, err := json.Marshal(arr)
	if err != nil {
		panic("err while re-marshaling 'Data' for map type conversion: " + err.Error())
	}
	tmp := make([]jaegerJSONModel.Trace, 0)
	if err = json.Unmarshal(bSlice, &tmp); err != nil {
		panic(err)
	}
	return tmp
}

// runPromscaleTraceQueryJSONServer starts a server that acts as a wrapper around promscale trace query module, responding in
// Jaeger JSON format. It reuses Jaeger's JSON layer as a wrapper so that the outputs from Jaeger's `/api` endpoints
// can be directly compared with Promscale's output from query endpoints.
func runPromscaleTraceQueryJSONServer(proxy *jaegerquery.Query, port string) io.Closer {
	promscaleQueryHandler := jaegerQueryApp.NewAPIHandler(jaegerQueryService.NewQueryService(
		proxy,
		proxy,
		jaegerQueryService.QueryServiceOptions{}))

	r := jaegerQueryApp.NewRouter()
	promscaleQueryHandler.RegisterRoutes(r)

	server := &http.Server{Handler: r}

	listener, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// Wait for server to be up.
	check := time.NewTicker(time.Second / 2)
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for {
		select {
		case <-timeout.Done():
			panic("Promscale trace JSON server could not be up within the given timeout")
		case <-check.C:
			// Simple get request.
			req, err := http.NewRequestWithContext(timeout, "GET", fmt.Sprintf("http://localhost%s/api/services", port), nil)
			if err != nil {
				panic(err)
			}

			client := http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				panic(err)
			}

			bSlice, err := ioutil.ReadAll(resp.Body)
			if err != nil || string(bSlice) == "" {
				panic(fmt.Errorf("empty bSlice or err: %w", err))
			}

			if resp.StatusCode == http.StatusOK {
				return server
			}
		}
	}
}

// Note: Promscale responds by default in GRPC. But, Jaeger responds in JSON. Since the final rendering layer of UI
// is JSON, we must compare both Promscale and Jaeger responses in JSON only. Hence, we wrap outputs from Promscale's GRPC API
// and use the code from Jaeger in.

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
	fetchTracesEndpoint = "%s/api/traces?start=%d&end=%d&service=%s&lookback=custom&limit=20&maxDuration&minDuration"
)

func getServices(t testing.TB, c httpClient) []string {
	r, err := do(fmt.Sprintf(servicesEndpoint, c.url))
	require.NoError(t, err)

	arr := convertToStringArr(r.Data.([]interface{}))
	sort.Strings(arr)
	return arr
}

func getOperations(t testing.TB, c httpClient, service string) []string {
	r, err := do(fmt.Sprintf(operationsEndpoint, c.url, service))
	require.NoError(t, err)

	arr := convertToStringArr(r.Data.([]interface{}))
	sort.Strings(arr)
	return arr
}

func getTrace(t testing.TB, c httpClient, traceId [16]byte) jaegerJSONModel.Trace {
	r, err := do(fmt.Sprintf(singleTraceEndpoint, c.url, getTraceId(traceId)))
	require.NoError(t, err)

	trace := convertToTrace(r.Data.([]interface{})[0])
	sortTraceContents(&trace)
	purgeProcessInformation(&trace)
	return trace
}

func getTraces(t testing.TB, c httpClient, service string, start, end int64) []jaegerJSONModel.Trace {
	r, err := do(fmt.Sprintf(fetchTracesEndpoint, c.url, start, end, service))
	require.NoError(t, err)

	traces := convertToTraces(r.Data.([]interface{}))
	sort.SliceStable(traces, func(i, j int) bool {
		return traces[i].TraceID < traces[j].TraceID
	})
	for i := range traces {
		sortTraceContents(&traces[i])
		purgeProcessInformation(&traces[i])
	}
	return traces
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
	if err = json.NewDecoder(bytes.NewReader(b)).Decode(&r); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &r, nil
}
