package jaeger_query

import (
	"context"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/model/pdata"
	"time"
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

// todo
// 1. tags
// 2. question: the received params are start time min & start time max of trace, but we are storing start time in & max of span, but since its an 'and' condition, its should be all good, right?

// real one
// const getTraces = `
// SELECT s.trace_id,
//        s.span_id,
//        s.parent_span_id,
//        s.start_time,
//        s.end_time,
// 	   s.end_time - s.start_time duration,
//        s.span_kind,
//        s.dropped_tags_count,
//        s.dropped_events_count,
//        s.dropped_link_count,
//        s.trace_state,
//        sch_url.url schema_url,
//        sn.name     span_name
// FROM   _ps_trace.span s
//        INNER JOIN _ps_trace.schema_url sch_url
//                ON s.resource_schema_url_id = sch_url.id
//        INNER JOIN _ps_trace.span_name sn
//                ON s.name_id = sn.id
// WHERE
// 		_ps_trace.val_text(s.resource_tags, 'service.name') = $1
// 	AND
// 		sn.name = $2
// 	AND
// 		s.start_time BETWEEN $3 AND $4
// 	AND
// 		s.duration BETWEEN $5 AND $6
// GROUP BY s.trace_id LIMIT $7
// 	`

const getTraces = `
SELECT s.trace_id,
	   array_agg(s.span_id) span_ids,
       array_agg(s.parent_span_id) parent_span_ids,
       array_agg(s.start_time) start_times,
       array_agg(s.end_time) end_times,
       array_agg(s.span_kind) span_kinds,
       array_agg(s.dropped_tags_count) dropped_tags_counts,
       array_agg(s.dropped_events_count) dropped_events_counts,
       array_agg(s.dropped_link_count) dropped_link_counts,
       array_agg(s.trace_state) trace_states,
       array_agg(sch_url.url) schema_urls,
       array_agg(sn.name)     span_names
FROM   _ps_trace.span s
       INNER JOIN _ps_trace.schema_url sch_url
               ON s.resource_schema_url_id = sch_url.id
       INNER JOIN _ps_trace.span_name sn
               ON s.name_id = sn.id
WHERE
		
		s.start_time BETWEEN $1::timestamptz AND $2::timestamptz
	AND
	(s.end_time - s.start_time) BETWEEN $3 AND $4
GROUP BY s.trace_id LIMIT $5`

func findTraces(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]model.Trace, error) {
	fmt.Println("query parameters", q)
	fmt.Println("operation name", q.OperationName)
	rawTracesIterator, err := conn.Query(ctx, getTraces, q.StartTimeMin, q.StartTimeMax, q.DurationMin, q.DurationMax, q.NumTraces)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rawTracesIterator.Close()

	var (
		//traces pdata.Traces

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

		// resourceSpans = traces.ResourceSpans()
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
		fmt.Println("traceId", traceId)

		b := traceId.Get().([]byte)
		var b16 [16]byte
		copy(b16[:16], b)
		traceIdStr := pdata.NewTraceID(b16).HexString()

		fmt.Println("traceId str", traceIdStr)
		fmt.Println("spanIds", spanIds)
		fmt.Println("startTimes", startTimes)
		fmt.Println("kinds", kinds)
		fmt.Println("schemaUrls", schemaUrls)

	}
	if err != nil {
		return nil, fmt.Errorf("iterating raw-traces: %w", err)
	}
	// query
	return nil, nil
}

func findTraceIDs(ctx context.Context, conn pgxconn.PgxConn, query *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	traceIds := make([]model.TraceID, 0)
	// query
	return traceIds, nil
}
