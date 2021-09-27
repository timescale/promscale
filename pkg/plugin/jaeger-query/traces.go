// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/model/pdata"

	"github.com/jackc/pgtype"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/storage_v1"
	jaegertranslator "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func singleTrace(ctx context.Context, conn pgxconn.PgxConn, traceID storage_v1.GetTraceRequest) (*model.Trace, error) {
	//traceIDstr := traceID.String()

	trace := new(model.Trace)
	//if err := conn.QueryRow(ctx, getTrace, traceID).Scan(trace); err != nil {
	//	return nil, fmt.Errorf("fetching a trace with %s as ID: %w", traceID.String(), err)
	//}
	// if err := batchToSingleTrace(trace, jaegerTrace); err != nil {
	// 	return nil, fmt.Errorf("batch to single trace: %w", err)
	// }
	return trace, nil
}

func findTraces(ctx context.Context, conn pgxconn.PgxConn, q *storage_v1.TraceQueryParameters) ([]*model.Batch, error) {
	query, hasDuration := buildQuery(q)
	var args []interface{}
	if hasDuration {
		args = []interface{}{q.DurationMin, q.DurationMax}
	}
	rawTracesIterator, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying traces: %w", err)
	}
	defer rawTracesIterator.Close()

	var (
		// Iteration vars.
		// Each element in the array corresponds to one span, of a trace.
		traceId             pgtype.UUID
		spanId              int64
		parentSpanId        pgtype.Int8
		startTime           time.Time
		endTime             time.Time
		kind                string
		droppedTagsCounts   int
		droppedEventsCounts int
		droppedLinkCounts   int
		traceState          pgtype.Text
		schemaUrl           pgtype.Text
		spanName            string

		resourceTags = make(map[string]interface{})
		spanTags     = make(map[string]interface{})

		traces = pdata.NewTraces()
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
			&spanId,
			&parentSpanId,
			&startTime,
			&endTime,
			&kind,
			&droppedTagsCounts,
			&droppedEventsCounts,
			&droppedLinkCounts,
			&traceState,
			&schemaUrl,
			&spanName,
			&resourceTags,
			&spanTags); err != nil {
			err = fmt.Errorf("scanning raw-traces: %w", err)
			break
		}

		if err = makeSpan(traces.ResourceSpans().AppendEmpty(),
			traceId, spanId, parentSpanId,
			startTime, endTime,
			kind,
			droppedTagsCounts, droppedEventsCounts, droppedLinkCounts,
			traceState, schemaUrl, spanName,
			resourceTags, spanTags); err != nil {
			return nil, fmt.Errorf("make span: %w", err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("iterating raw-traces: %w", err)
	}

	batch, err := jaegertranslator.InternalTracesToJaegerProto(traces)
	if err != nil {
		return nil, fmt.Errorf("internal-traces-to-jaeger-proto: %w", err)
	}
	return applyProcess(batch), nil
}

func applyProcess(b []*model.Batch) []*model.Batch {
	for i := range b {
		p := b[i].Process
		for j := range b[i].Spans {
			b[i].Spans[j].Process = p
		}
	}
	return b
}

// makeSpan makes a span by populating the pdata.Span with provided params.
func makeSpan(
	resourceSpan pdata.ResourceSpans,
	rawTraceId pgtype.UUID, rawId int64, rawParentId pgtype.Int8,
	startTime, endTime time.Time,
	kind string,
	droppedTagsCounts, droppedEventsCounts, droppedLinkCounts int,
	rawTraceState, rawSchemaUrl pgtype.Text,
	name string,
	resourceTags, spanTags map[string]interface{}) error {
	// todo: links, events
	resourceSpan.Resource().Attributes().InitFromMap(makeAttributes(resourceTags))

	// Type preprocessing.
	traceId, err := makeTraceId(rawTraceId)
	if err != nil {
		return fmt.Errorf("makeTraceId: %w", err)
	}

	id, err := makeSpanId(&rawId)
	if err != nil {
		return fmt.Errorf("id: makeSpanId: %w", err)
	}

	// We use a pointer since parent id can be nil. If we use normal int64, we can get parsing errors.
	var temp *int64
	if err := rawParentId.AssignTo(&temp); err != nil {
		return fmt.Errorf("rawParentId assign to: %w", err)
	}
	parentId, err := makeSpanId(temp) // todo
	if err != nil {
		return fmt.Errorf("parent-id: makeSpanId: %w", err)
	}

	traceState, err := textArraytoString(rawTraceState)
	if err != nil {
		return fmt.Errorf("traceState: text-to-string: %w", err)
	}
	schemaURL, err := textArraytoString(rawSchemaUrl)
	if err != nil {
		return fmt.Errorf("schemaURl: text-to-string: %w", err)
	}

	resourceSpan.SetSchemaUrl(schemaURL)

	instrumentationLibSpan := resourceSpan.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibSpan.SetSchemaUrl(schemaURL)

	// Populating a span.
	ref := instrumentationLibSpan.Spans().AppendEmpty()
	ref.SetTraceID(traceId)
	ref.SetSpanID(id)
	ref.SetParentSpanID(parentId)

	ref.SetName(name)
	ref.SetKind(makeKind(kind))

	ref.SetStartTimestamp(pdata.NewTimestampFromTime(startTime))
	ref.SetEndTimestamp(pdata.NewTimestampFromTime(endTime))

	ref.SetTraceState(pdata.TraceState(traceState))

	ref.SetDroppedAttributesCount(uint32(droppedTagsCounts))
	ref.SetDroppedEventsCount(uint32(droppedEventsCounts))
	ref.SetDroppedLinksCount(uint32(droppedLinkCounts))

	ref.Attributes().InitFromMap(makeAttributes(spanTags))

	return nil
}

// makeAttributes makes attribute map using tags.
func makeAttributes(tags map[string]interface{}) map[string]pdata.AttributeValue {
	m := make(map[string]pdata.AttributeValue, len(tags))
	// todo: attribute val as array?
	for k, v := range tags {
		switch val := v.(type) {
		case int64:
			m[k] = pdata.NewAttributeValueInt(val)
		case bool:
			m[k] = pdata.NewAttributeValueBool(val)
		case string:
			m[k] = pdata.NewAttributeValueString(val)
		case float64:
			m[k] = pdata.NewAttributeValueDouble(val)
		case []byte:
			m[k] = pdata.NewAttributeValueBytes(val)
		default:
			m[k] = pdata.NewAttributeValueEmpty()
		}
	}
	return m
}

func makeTraceId(s pgtype.UUID) (pdata.TraceID, error) {
	var bSlice [16]byte
	if err := s.AssignTo(&bSlice); err != nil {
		return pdata.TraceID{}, fmt.Errorf("trace id assign to: %w", err)
	}
	return pdata.NewTraceID(bSlice), nil
}

func makeSpanId(s *int64) (pdata.SpanID, error) {
	if s == nil {
		// Send an empty Span ID.
		return pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0}), nil
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(*s))
	var b8 [8]byte
	copy(b8[:8], b)

	return pdata.NewSpanID(b8), nil
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

func textArraytoString(s pgtype.Text) (string, error) {
	var d string
	if err := s.AssignTo(&d); err != nil {
		return "", fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}

func findTraceIDs(ctx context.Context, conn pgxconn.PgxConn, query *storage_v1.TraceQueryParameters) ([]model.TraceID, error) {
	traceIds := make([]model.TraceID, 0)
	// query
	return traceIds, nil
}
