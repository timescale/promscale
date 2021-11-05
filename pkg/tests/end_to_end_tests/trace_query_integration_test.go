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

	"go.opentelemetry.io/collector/model/pdata"
	"google.golang.org/grpc"

	jaegerproto "github.com/jaegertracing/jaeger/proto-gen/api_v2"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/pkg/timestamp"
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
	jaegerContainer, err := testhelpers.StartJaegerContainer(false)
	require.NoError(t, err)
	defer jaegerContainer.Close()

	sampleTraces := generateTestTrace()
	e2eDb := *testDatabase + "_e2e"

	withDB(t, e2eDb, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)

		// Ingest traces into Promscale.
		err = ingestor.IngestTraces(context.Background(), copyTraces(sampleTraces))
		require.NoError(t, err)
		defer ingestor.Close()

		err = insertDataIntoJaeger(fmt.Sprintf("localhost:%s", jaegerContainer.GrpcReceivingPort.Port()), copyTraces(sampleTraces))
		require.NoError(t, err)

		// Create Promscale proxy to query traces data.
		promscaleProxy := jaegerquery.New(pgxconn.NewPgxConn(db))

		shutdown := runPromscaleTraceQueryJSONServer(promscaleProxy, ":9203")
		defer shutdown.Close()

		promscaleClient := httpClient{"http://localhost:9203"}
		jaegerClient := httpClient{"http://localhost:" + jaegerContainer.UIPort.Port()}

		validateStringSliceResp := func(promscaleResp, jaegerResp *structuredResponse) {
			a := convertToStringArr(promscaleResp.Data.([]interface{}))
			b := convertToStringArr(jaegerResp.Data.([]interface{}))

			sort.Strings(a)
			sort.Strings(b)

			require.Equal(t, b, a)
		}

		// Verify services.
		promscaleResp, err := promscaleClient.getServices()
		require.NoError(t, err)

		jaegerResp, err := jaegerClient.getServices()
		require.NoError(t, err)

		validateStringSliceResp(promscaleResp, jaegerResp)

		// Verify Operations.
		promscaleResp, err = promscaleClient.getOperations(service0)
		require.NoError(t, err)

		jaegerResp, err = jaegerClient.getOperations(service0)
		require.NoError(t, err)

		validateStringSliceResp(promscaleResp, jaegerResp)

		// Verify a single trace fetch.
		promscaleResp, err = promscaleClient.getTrace(getTraceId(traceID1))
		require.NoError(t, err)
		pTrace := convertToTrace(promscaleResp.Data.([]interface{})[0])

		jaegerResp, err = jaegerClient.getTrace(getTraceId(traceID1))
		require.NoError(t, err)
		jTrace := convertToTrace(jaegerResp.Data.([]interface{})[0])

		sortTraceContents(&jTrace)
		sortTraceContents(&pTrace)

		require.Exactly(t, jTrace, pTrace)

		// Verify fetch traces API.
		traceStart := timestamp.FromTime(testSpanStartTime) * 1000
		traceEnd := timestamp.FromTime(testSpanEndTime) * 1000

		promscaleResp, err = promscaleClient.fetchTraces(traceStart, traceEnd, service0)
		require.NoError(t, err)
		pTraces := convertToTraces(promscaleResp.Data.([]interface{}))

		jaegerResp, err = jaegerClient.fetchTraces(traceStart, traceEnd, service0)
		require.NoError(t, err)
		jTraces := convertToTraces(jaegerResp.Data.([]interface{}))

		// Sort traces.
		sort.SliceStable(pTraces, func(i, j int) bool {
			return pTraces[i].TraceID < pTraces[j].TraceID
		})
		sort.SliceStable(jTraces, func(i, j int) bool {
			return jTraces[i].TraceID < jTraces[j].TraceID
		})

		require.Equal(t, len(jTraces), len(pTraces), "num of traces returned in fetch traces API")

		// Sort the trace contents and prepare them to compare.
		for i := 0; i < len(jTraces); i++ {
			a := &jTraces[i]
			b := &pTraces[i]

			require.Equalf(t, len(a.Spans), len(b.Spans), "num spans of %d", i)
			require.Equalf(t, a.TraceID, b.TraceID, "trace id of %d", i)

			sortTraceContents(a)
			sortTraceContents(b)

			purgeProcessInformation(a)
			purgeProcessInformation(b)
		}
		require.Exactly(t, jTraces, pTraces)
	})
}

func insertDataIntoJaeger(endpoint string, data pdata.Traces) error {
	opts := []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		panic(err)
	}

	client := jaegerproto.NewCollectorServiceClient(conn)

	batches, err := jaegertranslator.InternalTracesToJaegerProto(data)
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

	runServer := func() {
		listener, err := net.Listen("tcp", port)
		if err != nil {
			panic(err)
		}
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}

	go runServer()

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
	fetchTraces         = "%s/api/traces?start=%d&end=%d&service=%s&lookback=custom&limit=20&maxDuration&minDuration"
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
	resp, err := do(fmt.Sprintf(singleTraceEndpoint, c.url, traceId))
	if err != nil {
		return nil, fmt.Errorf("do: %w", err)
	}
	return resp, nil
}

func (c httpClient) fetchTraces(start, end int64, service string) (*structuredResponse, error) {
	resp, err := do(fmt.Sprintf(fetchTraces, c.url, start, end, service))
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
	if err = json.NewDecoder(bytes.NewReader(b)).Decode(&r); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}
	return &r, nil
}
