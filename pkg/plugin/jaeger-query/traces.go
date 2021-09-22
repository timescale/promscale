package jaeger_query

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/model/pdata"
)

const (
	getTrace = "select 1"
)

func singleTrace(ctx context.Context, conn pgxconn.PgxConn, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	//traceIDstr := traceID.String()

	trace := new(model.Trace)
	//if err := conn.QueryRow(ctx, getTrace, traceID).Scan(trace); err != nil {
	//	return nil, fmt.Errorf("fetching a trace with %s as ID: %w", traceID.String(), err)
	//}
	sample := prepareDemoTrace()
	jaegerTrace, err := toJaeger(sample)
	if err != nil {
		return nil, fmt.Errorf("converting to jaeger trace: %w", err)
	}
	if err = batchToSingleTrace(trace, jaegerTrace); err != nil {
		return nil, fmt.Errorf("batch to single trace: %w", err)
	}
	return trace, nil
}

func batchToSingleTrace(trace *model.Trace, batch []*model.Batch) error {
	if len(batch) == 0 {
		return fmt.Errorf("empty batch")
	}
	if len(batch) > 1 {
		// We are asked to send one trace, since a single TraceID can have only a single element in batch.
		// If more than one, there are semantic issues with this trace, hence error out.
		return fmt.Errorf("a single TraceID must contain a single batch of spans. But, found %d", len(batch))
	}
	trace.Spans = batch[0].Spans
	return nil
}

func toJaeger(pTraces pdata.Traces) ([]*model.Batch, error) {
	jaegerTrace, err := jaegertranslator.InternalTracesToJaegerProto(pTraces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	return jaegerTrace, nil
}

func prepareDemoTrace() pdata.Traces {
	td := pdata.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("http://schema_url")
	s := rs.InstrumentationLibrarySpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName("mock_span")
	traceID := pdata.NewTraceID([16]byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	s.SetTraceID(traceID)
	emptySpanID := pdata.NewSpanID([8]byte{0, 0, 1, 0, 0, 0, 1, 0})
	s.SetSpanID(emptySpanID)
	startTime := pdata.NewTimestampFromTime(time.Now())
	s.SetStartTimestamp(startTime)
	endTime := pdata.NewTimestampFromTime(time.Now().Add(time.Minute))
	s.SetEndTimestamp(endTime)
	s.SetKind(pdata.SpanKindConsumer)

	s.SetParentSpanID(emptySpanID)
	s.SetDroppedAttributesCount(1)
	s.SetDroppedEventsCount(1)
	s.SetDroppedLinksCount(1)
	s.SetTraceState("tracestate_1")
	return td
}

func findTraces(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]*model.Batch, error) {
	fmt.Println("query parameters", q)

	builder := newFindtracesQueryBuilder()
	if len(q.ServiceName) > 0 {
		q.ServiceName = q.ServiceName[1 : len(q.ServiceName)-1] // temporary, based on trace gen behaviour
		builder.withWhere().withServiceName(q.ServiceName)
	}
	if len(q.OperationName) > 0 {
		builder.withOperationName(q.OperationName)
	}
	if len(q.Tags) > 0 {
		builder.withResourceTags(q.Tags)
	}
	var defaultTime time.Time
	if q.StartTimeMin != defaultTime && q.StartTimeMax != defaultTime {
		builder.withStartRange(q.StartTimeMin, q.StartTimeMax)
	}
	var defaultDuration time.Duration
	if q.DurationMin != defaultDuration && q.DurationMax != defaultDuration {
		builder.withDurationRange(q.DurationMin, q.DurationMax)
	}
	if q.NumTraces == 0 {
		builder.groupBy()
	} else {
		builder.groupBy().withNumTraces(q.NumTraces)
	}

	query := builder.query()
	fmt.Println("build query=>\n", query)

	rawTracesIterator, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rawTracesIterator.Close()

	var (
		// Iteration vars.
		// Each element in the array corresponds to one span, of a trace.
		traceId             pgtype.Bytea // The value is received in uuid.
		spanIds             pgtype.Int8Array
		parentSpanIds       pgtype.Int8Array
		startTimes          pgtype.TimestamptzArray
		endTimes            pgtype.TimestamptzArray
		kinds               pgtype.TextArray
		droppedTagsCounts   pgtype.Int4Array
		droppedEventsCounts pgtype.Int4Array
		droppedLinkCounts   pgtype.Int4Array
		traceStates         pgtype.TextArray
		schemaUrls          pgtype.TextArray
		spanNames           pgtype.TextArray

		traces        = pdata.NewTraces()
		resourceSpans = traces.ResourceSpans().AppendEmpty()
	)
	for rawTracesIterator.Next() {
		// Each iteration in this block represents one trace.
		// rawTraces
		if rawTracesIterator.Err() != nil {
			err = fmt.Errorf("raw-traces iterator: %w", rawTracesIterator.Err())
			break
		}
		// Each scan is a scan for a complete Trace. This means,
		// it contains data from multiple spans, and hence, an array.
		if err = rawTracesIterator.Scan(
			&traceId,
			&spanIds,
			&parentSpanIds,
			&startTimes,
			&endTimes,
			&kinds,
			&droppedTagsCounts,
			&droppedEventsCounts,
			&droppedLinkCounts,
			&traceStates,
			&schemaUrls,
			&spanNames); err != nil {
			err = fmt.Errorf("scanning raw-traces: %w", err)
			break
		}

		libSpans := resourceSpans.InstrumentationLibrarySpans().AppendEmpty()

		spanIds_, err := makeSpanIds(spanIds)
		if err != nil {
			return nil, fmt.Errorf("span-ids: make-span-ids: %w", err)
		}

		parentSpanIds_, err := makeSpanIds(parentSpanIds) // todo
		if err != nil {
			return nil, fmt.Errorf("parent-span-ids: make-span-ids: %w", err)
		}

		startTimes_, err := timestamptzArraytoTimeArr(startTimes)
		if err != nil {
			return nil, fmt.Errorf("start time: timestamptz-array-to-time-array: %w", err)
		}

		endTimes_, err := timestamptzArraytoTimeArr(endTimes)
		if err != nil {
			return nil, fmt.Errorf("start time: timestamptz-array-to-time-array: %w", err)
		}

		droppedTagsCounts_, err := int4ArraytoIntArr(droppedTagsCounts)
		if err != nil {
			return nil, fmt.Errorf("droppedTagsCounts: int4ArraytoIntArr: %w", err)
		}
		droppedEventsCounts_, err := int4ArraytoIntArr(droppedEventsCounts)
		if err != nil {
			return nil, fmt.Errorf("droppedEventsCounts: int4ArraytoIntArr: %w", err)
		}
		droppedLinkCounts_, err := int4ArraytoIntArr(droppedLinkCounts)
		if err != nil {
			return nil, fmt.Errorf("droppedTagsCounts: int4ArraytoIntArr: %w", err)
		}

		kinds_, err := textArraytoStringArr(kinds)
		if err != nil {
			return nil, fmt.Errorf("kinds: text-array-to-string-array: %w", err)
		}
		traceStates_, err := textArraytoStringArr(traceStates)
		if err != nil {
			return nil, fmt.Errorf("traceStates: text-array-to-string-array: %w", err)
		}
		schemaUrls_, err := textArraytoStringArr(schemaUrls)
		if err != nil {
			return nil, fmt.Errorf("schemaUrls: text-array-to-string-array: %w", err)
		}
		spanNames_, err := textArraytoStringArr(spanNames)
		if err != nil {
			return nil, fmt.Errorf("spanNames: text-array-to-string-array: %w", err)
		}

		makeSpans(libSpans,
			makeTraceId(traceId),
			spanIds_,
			parentSpanIds_,
			startTimes_,
			endTimes_,
			kinds_,
			droppedTagsCounts_,
			droppedEventsCounts_,
			droppedLinkCounts_,
			traceStates_,
			schemaUrls_,
			spanNames_)
	}
	if err != nil {
		return nil, fmt.Errorf("iterating raw-traces: %w", err)
	}
	fmt.Println("total spans are", traces.SpanCount())
	batch, err := jaegertranslator.InternalTracesToJaegerProto(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	fmt.Println("elements in batch", len(batch))
	return batch, nil
}

func int8ArraytoInt64Arr(s pgtype.Int8Array) ([]*int64, error) {
	var d []*int64
	if err := s.AssignTo(&d); err != nil {
		return nil, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}

func int4ArraytoIntArr(s pgtype.Int4Array) ([]int32, error) {
	var d []int32
	if err := s.AssignTo(&d); err != nil {
		return nil, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}

func textArraytoStringArr(s pgtype.TextArray) ([]string, error) {
	var d []string
	if err := s.AssignTo(&d); err != nil {
		return nil, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}

func timestamptzArraytoTimeArr(s pgtype.TimestamptzArray) ([]time.Time, error) {
	var d []time.Time
	if err := s.AssignTo(&d); err != nil {
		return nil, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}

func makeSpans(
	libSpans pdata.InstrumentationLibrarySpans,
	traceId pdata.TraceID, spanIds, parentSpanIds []pdata.SpanID,
	startTime, endTime []time.Time, spanKind []string,
	droppedTagsCounts, droppedEventsCounts, droppedLinkCounts []int32,
	traceStates, schemaUrls, spanNames []string) {
	// todo: schemaUrls
	num := len(spanIds)
	for i := 0; i < num; i++ {
		s := libSpans.Spans().AppendEmpty()
		s.SetTraceID(traceId)
		s.SetSpanID(spanIds[i])
		s.SetParentSpanID(parentSpanIds[i])

		s.SetStartTimestamp(pdata.NewTimestampFromTime(startTime[i]))
		s.SetEndTimestamp(pdata.NewTimestampFromTime(endTime[i]))
		s.SetKind(makeKind(spanKind[i]))

		s.SetDroppedAttributesCount(uint32(droppedTagsCounts[i]))
		s.SetDroppedEventsCount(uint32(droppedEventsCounts[i]))
		s.SetDroppedLinksCount(uint32(droppedLinkCounts[i]))

		s.SetTraceState(pdata.TraceState(traceStates[i]))
		s.SetName(spanNames[i])
	}
}

func makeTraceId(s pgtype.Bytea) pdata.TraceID {
	b := s.Get().([]byte)
	var b16 [16]byte
	copy(b16[:16], b)
	return pdata.NewTraceID(b16)
}

func makeSpanIds(s pgtype.Int8Array) ([]pdata.SpanID, error) {
	tmp, err := int8ArraytoInt64Arr(s)
	if err != nil {
		return nil, fmt.Errorf("int8ArraytoInt64Arr: %w", err)
	}
	b := make([]byte, 8)
	spanIds := make([]pdata.SpanID, len(tmp))
	for i := range tmp {
		if tmp[i] == nil {
			spanIds[i] = pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Empty span id.
			continue
		}
		binary.BigEndian.PutUint64(b, uint64(*tmp[i]))
		var b8 [8]byte
		copy(b8[:8], b)
		spanIds[i] = pdata.NewSpanID(b8)
	}

	return spanIds, nil
}

func makeKind(s string) pdata.SpanKind {
	switch s {
	case "SPAN_KIND_CLIENT":
		return pdata.SpanKindClient
	case "SPAN_KIND_SERVER":
		return pdata.SpanKindServer
	case "SPAN_KIND_INTERNAL":
		return pdata.SpanKindInternal
	case "SPAN_KIND_CONSUMER":
		return pdata.SpanKindConsumer
	case "SPAN_KIND_PRODUCER":
		return pdata.SpanKindProducer
	default:
		return pdata.SpanKindUnspecified
	}
}

func findTraceIDs(ctx context.Context, conn pgxconn.PgxConn, query *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	traceIds := make([]model.TraceID, 0)
	// query
	return traceIds, nil
}
