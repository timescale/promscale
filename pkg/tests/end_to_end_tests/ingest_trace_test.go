package end_to_end_tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/jaeger/query"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/model/pdata"
)

var (
	traceID1               = [16]byte{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6'}
	traceID2               = [16]byte{'0', '2', '3', '4', '0', '6', '7', '8', '9', '0', '0', '2', '3', '4', '0', '6'}
	testSpanStartTime      = time.Date(2020, 2, 11, 20, 26, 12, 321000, time.UTC)
	testSpanStartTimestamp = pdata.NewTimestampFromTime(testSpanStartTime)

	testSpanEventTime      = time.Date(2020, 2, 11, 20, 26, 13, 123000, time.UTC)
	testSpanEventTimestamp = pdata.NewTimestampFromTime(testSpanEventTime)

	testSpanEndTime      = time.Date(2020, 2, 11, 20, 26, 13, 789000, time.UTC)
	testSpanEndTimestamp = pdata.NewTimestampFromTime(testSpanEndTime)

	resourceAttributes = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{
		"resource-attr": pdata.NewAttributeValueString("resource-attr-val-1"),
		"service.name":  pdata.NewAttributeValueString("service1"),
	})
	spanAttributes      = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-attr": pdata.NewAttributeValueString("span-attr-val")})
	spanEventAttributes = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-event-attr": pdata.NewAttributeValueString("span-event-attr-val")})
	spanLinkAttributes  = pdata.NewAttributeMapFromMap(map[string]pdata.AttributeValue{"span-link-attr": pdata.NewAttributeValueString("span-link-attr-val")})
)

func TestIngestTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)
	})
}

func generateTestTrace() pdata.Traces {
	spanCount := 4
	td := pdata.NewTraces()
	td.ResourceSpans().AppendEmpty()
	rs0 := td.ResourceSpans().At(0)
	initResourceAttributes1(rs0.Resource().Attributes())
	td.ResourceSpans().At(0).InstrumentationLibrarySpans().AppendEmpty()
	initInstLib(td.ResourceSpans().At(0).InstrumentationLibrarySpans())
	rs0ils0 := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0)
	for i := 0; i < spanCount; i++ {
		fillSpanOne(rs0ils0.Spans().AppendEmpty())
	}
	for i := 0; i < spanCount; i++ {
		fillSpanTwo(rs0ils0.Spans().AppendEmpty())
	}

	return td
}

func initInstLib(dest pdata.InstrumentationLibrarySpansSlice) {
	dest.At(0).SetSchemaUrl("test")
	dest.At(0).InstrumentationLibrary().SetName("inst_lib")
	dest.At(0).InstrumentationLibrary().SetVersion("1")
}

func initResourceAttributes1(dest pdata.AttributeMap) {
	dest.Clear()
	resourceAttributes.CopyTo(dest)
}

func initSpanEventAttributes(dest pdata.AttributeMap) {
	dest.Clear()
	spanEventAttributes.CopyTo(dest)
}

func initSpanAttributes(dest pdata.AttributeMap) {
	dest.Clear()
	spanAttributes.CopyTo(dest)
}

func initSpanLinkAttributes(dest pdata.AttributeMap) {
	dest.Clear()
	spanLinkAttributes.CopyTo(dest)
}

func generateRandSpanID() (result [8]byte) {
	rand.Read(result[:])
	return result
}

func fillSpanOne(span pdata.Span) {
	span.SetTraceID(pdata.NewTraceID(traceID1))
	span.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
	span.SetName("operationA")
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
	span.SetDroppedAttributesCount(1)
	span.SetTraceState("span-trace-state1")
	initSpanAttributes(span.Attributes())
	evs := span.Events()
	ev0 := evs.AppendEmpty()
	ev0.SetTimestamp(testSpanEventTimestamp)
	ev0.SetName("event-with-attr")
	initSpanEventAttributes(ev0.Attributes())
	ev0.SetDroppedAttributesCount(2)
	ev1 := evs.AppendEmpty()
	ev1.SetTimestamp(testSpanEventTimestamp)
	ev1.SetName("event")
	ev1.SetDroppedAttributesCount(2)
	span.SetDroppedEventsCount(1)
	status := span.Status()
	status.SetCode(pdata.StatusCodeError)
	status.SetMessage("status-cancelled")
}

func fillSpanTwo(span pdata.Span) {
	span.SetTraceID(pdata.NewTraceID(traceID2))
	span.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
	span.SetName("operationB")
	span.SetStartTimestamp(testSpanStartTimestamp)
	span.SetEndTimestamp(testSpanEndTimestamp)
	span.SetTraceState("span-trace-state2")
	initSpanAttributes(span.Attributes())
	link0 := span.Links().AppendEmpty()
	initSpanLinkAttributes(link0.Attributes())
	link0.SetTraceID(pdata.NewTraceID([16]byte{'1'}))
	link0.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
	link0.SetDroppedAttributesCount(4)
	link0.SetTraceState("link-trace-state1")
	link1 := span.Links().AppendEmpty()
	link1.SetDroppedAttributesCount(4)
	link1.SetTraceID(pdata.NewTraceID([16]byte{'1'}))
	link1.SetSpanID(pdata.NewSpanID(generateRandSpanID()))
	link1.SetTraceState("link-trace-state2")
	span.SetDroppedLinksCount(3)
}

func TestQueryTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)

		q := query.New(pgxconn.NewQueryLoggingPgxConn(db))

		getOperationsTest(t, q)
		findTraceTest(t, q)

	})
}

func getOperationsTest(t testing.TB, q *query.Query) {
	request := spanstore.OperationQueryParameters{
		ServiceName: "service1",
	}
	ops, err := q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "service never existed",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "service1",
		SpanKind:    "SPAN_KIND_UNSPECIFIED",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "service1",
		SpanKind:    "SPAN_KIND_CLIENT",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(ops))
}

func findTraceTest(t testing.TB, q *query.Query) {
	request := &spanstore.TraceQueryParameters{
		ServiceName: "service1",
	}

	traces, err := q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
	}

	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val-not-actually",
		},
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime.Add(time.Second),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime.Add(-time.Second),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime) + (1 * time.Millisecond),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
		DurationMax:  testSpanEndTime.Sub(testSpanStartTime),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
		DurationMax:  testSpanEndTime.Sub(testSpanStartTime) - time.Millisecond,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service1",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
		DurationMax:  testSpanEndTime.Sub(testSpanStartTime),
		NumTraces:    2,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))
}
