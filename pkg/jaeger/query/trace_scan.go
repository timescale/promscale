// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jackc/pgtype"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor/trace"
	"github.com/timescale/promscale/pkg/pgxconn"
)

type spanDBResult struct {
	traceId             pgtype.UUID
	spanId              int64
	parentSpanId        pgtype.Int8
	startTime           time.Time
	endTime             time.Time
	kind                pgtype.Text
	droppedTagsCounts   int
	droppedEventsCounts int
	droppedLinkCounts   int
	statusCode          string
	statusMessage       pgtype.Text
	traceState          pgtype.Text
	schemaUrl           pgtype.Text
	spanName            string
	resourceTags        pgtype.JSONB
	spanTags            pgtype.JSONB

	// From events table.
	// for events, the entire slice can be nil but not any element within the slice
	eventNames            *[]string
	eventTimes            *[]time.Time
	eventDroppedTagsCount *[]int
	eventTags             pgtype.JSONBArray

	// From instrumentation lib table.
	instLibName      *string
	instLibVersion   *string
	instLibSchemaUrl *string

	// From link table.
	linksLinkedTraceIds   pgtype.UUIDArray
	linksLinkedSpanIds    *[]int64
	linksTraceStates      *[]*string
	linksDroppedTagsCount *[]int
	linksTags             pgtype.JSONBArray
}

func ScanRow(row pgxconn.PgxRows, traces *pdata.Traces) error {
	dbRes := spanDBResult{}

	if err := row.Scan(
		// Span table.
		&dbRes.traceId,
		&dbRes.spanId,
		&dbRes.parentSpanId,
		&dbRes.startTime,
		&dbRes.endTime,
		&dbRes.kind,
		&dbRes.droppedTagsCounts,
		&dbRes.droppedEventsCounts,
		&dbRes.droppedLinkCounts,
		&dbRes.statusCode,
		&dbRes.statusMessage,
		&dbRes.traceState,
		&dbRes.schemaUrl,
		&dbRes.spanName,
		&dbRes.resourceTags,
		&dbRes.spanTags,

		// Event table.
		&dbRes.eventNames,
		&dbRes.eventTimes,
		&dbRes.eventDroppedTagsCount,
		&dbRes.eventTags,

		// Instrumentation lib table.
		&dbRes.instLibName,
		&dbRes.instLibVersion,
		&dbRes.instLibSchemaUrl,

		// Link table.
		&dbRes.linksLinkedTraceIds,
		&dbRes.linksLinkedSpanIds,
		&dbRes.linksTraceStates,
		&dbRes.linksDroppedTagsCount,
		&dbRes.linksTags,
	); err != nil {
		return fmt.Errorf("scanning traces: %w", err)
	}

	span := traces.ResourceSpans().AppendEmpty()
	if err := populateSpan(span, &dbRes); err != nil {
		return fmt.Errorf("populate span error: %w", err)
	}

	return nil
}

func populateSpan(
	// From span table.
	resourceSpan pdata.ResourceSpans,
	dbResult *spanDBResult) error {

	attr, err := makeAttributes(dbResult.resourceTags)
	if err != nil {
		return fmt.Errorf("making resource tags: %w", err)
	}
	pdata.NewAttributeMapFromMap(attr).CopyTo(resourceSpan.Resource().Attributes())

	instrumentationLibSpan := resourceSpan.ScopeSpans().AppendEmpty()
	if dbResult.instLibSchemaUrl != nil {
		instrumentationLibSpan.SetSchemaUrl(*dbResult.instLibSchemaUrl)
	}

	instLib := instrumentationLibSpan.Scope()
	if dbResult.instLibName != nil {
		instLib.SetName(*dbResult.instLibName)
	}
	if dbResult.instLibVersion != nil {
		instLib.SetVersion(*dbResult.instLibVersion)
	}

	// Populating a span.
	ref := instrumentationLibSpan.Spans().AppendEmpty()

	// Type preprocessing.
	traceId, err := makeTraceId(dbResult.traceId)
	if err != nil {
		return fmt.Errorf("makeTraceId: %w", err)
	}
	ref.SetTraceID(traceId)

	id := makeSpanId(&dbResult.spanId)
	ref.SetSpanID(id)

	// We use a pointer since parent id can be nil. If we use normal int64, we can get parsing errors.
	var temp *int64
	if err := dbResult.parentSpanId.AssignTo(&temp); err != nil {
		return fmt.Errorf("assigning parent span id: %w", err)
	}
	parentId := makeSpanId(temp)
	ref.SetParentSpanID(parentId)

	if dbResult.traceState.Status == pgtype.Present {
		ref.SetTraceState(pdata.TraceState(dbResult.traceState.String))
	}

	if dbResult.schemaUrl.Status == pgtype.Present {
		resourceSpan.SetSchemaUrl(dbResult.schemaUrl.String)
	}

	ref.SetName(dbResult.spanName)

	if dbResult.kind.Status == pgtype.Present {
		kind, err := makeKind(dbResult.kind.String)
		if err != nil {
			return err
		}
		ref.SetKind(kind)
	}

	ref.SetStartTimestamp(pdata.NewTimestampFromTime(dbResult.startTime))
	ref.SetEndTimestamp(pdata.NewTimestampFromTime(dbResult.endTime))

	ref.SetDroppedAttributesCount(uint32(dbResult.droppedTagsCounts))
	ref.SetDroppedEventsCount(uint32(dbResult.droppedEventsCounts))
	ref.SetDroppedLinksCount(uint32(dbResult.droppedLinkCounts))

	if err = setStatus(ref, dbResult); err != nil {
		return fmt.Errorf("set status: %w", err)
	}

	attr, err = makeAttributes(dbResult.spanTags)
	if err != nil {
		return fmt.Errorf("making span tags: %w", err)
	}
	pdata.NewAttributeMapFromMap(attr).CopyTo(ref.Attributes())

	if dbResult.eventNames != nil {
		if err := populateEvents(ref.Events(), dbResult); err != nil {
			return fmt.Errorf("populate events error: %w", err)
		}
	}
	if dbResult.linksLinkedSpanIds != nil {
		if err := populateLinks(ref.Links(), dbResult); err != nil {
			return fmt.Errorf("populate links error: %w", err)
		}
	}
	return nil
}

func setStatus(ref ptrace.Span, dbRes *spanDBResult) error {
	if dbRes.statusCode != "" {
		code, err := makeStatusCode(dbRes.statusCode)
		if err != nil {
			return err
		}
		ref.Status().SetCode(code)
	}
	if dbRes.statusMessage.Status != pgtype.Null {
		message := dbRes.statusMessage.String
		ref.Status().SetMessage(message)
	}
	return nil
}

func populateEvents(
	spanEventSlice pdata.SpanEventSlice,
	dbResult *spanDBResult) error {

	n := len(*dbResult.eventNames)
	for i := 0; i < n; i++ {
		event := spanEventSlice.AppendEmpty()
		event.SetName((*dbResult.eventNames)[i])
		event.SetTimestamp(pdata.NewTimestampFromTime((*dbResult.eventTimes)[i]))
		event.SetDroppedAttributesCount(uint32((*dbResult.eventDroppedTagsCount)[i]))
		attr, err := makeAttributes(dbResult.eventTags.Elements[i])
		if err != nil {
			return fmt.Errorf("making event tags: %w", err)
		}
		pdata.NewAttributeMapFromMap(attr).CopyTo(event.Attributes())
	}
	return nil
}

func populateLinks(
	spanEventSlice pdata.SpanLinkSlice,
	dbResult *spanDBResult) error {

	n := len(*dbResult.linksLinkedSpanIds)

	var linkedTraceIds [][16]byte
	if err := dbResult.linksLinkedTraceIds.AssignTo(&linkedTraceIds); err != nil {
		return fmt.Errorf("linksLinkedTraceIds: AssignTo: %w", err)
	}

	for i := 0; i < n; i++ {
		link := spanEventSlice.AppendEmpty()

		link.SetTraceID(pdata.NewTraceID(linkedTraceIds[i]))

		spanId := makeSpanId(&(*dbResult.linksLinkedSpanIds)[i])
		link.SetSpanID(spanId)

		if (*dbResult.linksTraceStates)[i] != nil {
			traceState := *((*dbResult.linksTraceStates)[i])
			link.SetTraceState(pdata.TraceState(traceState))
		}
		link.SetDroppedAttributesCount(uint32((*dbResult.linksDroppedTagsCount)[i]))
		attr, err := makeAttributes(dbResult.linksTags.Elements[i])
		if err != nil {
			return fmt.Errorf("making link tags: %w", err)
		}
		pdata.NewAttributeMapFromMap(attr).CopyTo(link.Attributes())
	}
	return nil
}

// makeAttributes makes attribute map using tags.
func makeAttributes(tagsJson pgtype.JSONB) (map[string]pcommon.Value, error) {
	var tags map[string]interface{}
	if err := tagsJson.AssignTo(&tags); err != nil {
		return nil, fmt.Errorf("tags assign to: %w", err)
	}
	otMap := pcommon.NewMapFromRaw(tags)
	m := make(map[string]pcommon.Value, len(tags))
	populateMap := func(k string, v pcommon.Value) bool {
		m[k] = v
		return true
	}
	otMap.Range(populateMap)
	return m, nil
}

func makeTraceId(s pgtype.UUID) (pdata.TraceID, error) {
	var bSlice [16]byte
	if err := s.AssignTo(&bSlice); err != nil {
		return pdata.TraceID{}, fmt.Errorf("trace id assign to: %w", err)
	}
	return pdata.NewTraceID(bSlice), nil
}

func makeSpanId(s *int64) pdata.SpanID {
	if s == nil {
		// Send an empty Span ID.
		return pdata.NewSpanID([8]byte{})
	}

	b8 := trace.Int64ToByteArray(*s)
	return pdata.NewSpanID(b8)
}

func getPGKindEnum(jaegerKind string) (string, error) {
	switch jaegerKind {
	case ptrace.SpanKindClient.String():
		return "client", nil
	case ptrace.SpanKindServer.String():
		return "server", nil
	case ptrace.SpanKindInternal.String():
		return "internal", nil
	case ptrace.SpanKindConsumer.String():
		return "consumer", nil
	case ptrace.SpanKindProducer.String():
		return "producer", nil
	case ptrace.SpanKindUnspecified.String():
		return "unspecified", nil
	default:
		return "", fmt.Errorf("unknown span kind: %v", jaegerKind)
	}
}

func makeKind(s string) (pdata.SpanKind, error) {
	switch s {
	case "client":
		return ptrace.SpanKindClient, nil
	case "server":
		return ptrace.SpanKindServer, nil
	case "internal":
		return ptrace.SpanKindInternal, nil
	case "consumer":
		return ptrace.SpanKindConsumer, nil
	case "producer":
		return ptrace.SpanKindProducer, nil
	case "unspecified":
		return ptrace.SpanKindUnspecified, nil
	default:
		return ptrace.SpanKindUnspecified, fmt.Errorf("unknown span kind: %s", s)
	}
}

func makeStatusCode(s string) (pdata.StatusCode, error) {
	switch s {
	case "ok":
		return ptrace.StatusCodeOk, nil
	case "error":
		return ptrace.StatusCodeError, nil
	case "unset":
		return ptrace.StatusCodeUnset, nil
	default:
		return ptrace.StatusCodeUnset, fmt.Errorf("unknown status-code kind: %s", s)
	}
}
