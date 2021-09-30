// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package jaeger_query

import (
	"encoding/binary"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/model/pdata"

	"github.com/jackc/pgtype"
)

// makeSpan makes a span by populating the pdata.ResourceSpans with provided params.
func makeSpan(
	// From span table.
	resourceSpan pdata.ResourceSpans,
	rawTraceId pgtype.UUID,
	rawId int64,
	rawParentId pgtype.Int8,
	startTime,
	endTime time.Time,
	rawKind pgtype.Text,
	droppedTagsCounts, droppedEventsCounts, droppedLinkCounts int,
	rawTraceState, rawSchemaUrl pgtype.Text,
	name string,
	resourceTags, spanTags map[string]interface{},

	// From event table.
	eventNames *[]*string,
	eventTimes *[]*time.Time,
	eventDroppedTagsCount *[]*int,
	eventTags []map[string]interface{},

	// From instrumentation lib table.
	instName *string,
	instVersion *string,
	instSchemaUrl *string,

	// From link table.
	linksLinkedTraceIds pgtype.UUIDArray,
	linksLinkedSpanIds *[]*int64,
	linksTraceStates *[]*string,
	linksDroppedTagsCount *[]*int,
	linksTags []map[string]interface{}) error {
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

	resourceSpan.SetSchemaUrl(stringOrEmpty(schemaURL))

	instrumentationLibSpan := resourceSpan.InstrumentationLibrarySpans().AppendEmpty()
	instrumentationLibSpan.SetSchemaUrl(stringOrEmpty(instSchemaUrl))

	instLib := instrumentationLibSpan.InstrumentationLibrary()
	instLib.SetName(stringOrEmpty(instName))
	instLib.SetVersion(stringOrEmpty(instVersion))

	// Populating a span.
	ref := instrumentationLibSpan.Spans().AppendEmpty()
	ref.SetTraceID(traceId)
	ref.SetSpanID(id)
	ref.SetParentSpanID(parentId)

	ref.SetName(name)

	kind, err := textArraytoString(rawKind)
	if err != nil {
		return fmt.Errorf("kind: text-to-string: %w", err)
	}
	ref.SetKind(makeKind(stringOrEmpty(kind)))

	ref.SetStartTimestamp(pdata.NewTimestampFromTime(startTime))
	ref.SetEndTimestamp(pdata.NewTimestampFromTime(endTime))

	ref.SetTraceState(pdata.TraceState(stringOrEmpty(traceState)))

	ref.SetDroppedAttributesCount(uint32(droppedTagsCounts))
	ref.SetDroppedEventsCount(uint32(droppedEventsCounts))
	ref.SetDroppedLinksCount(uint32(droppedLinkCounts))

	ref.Attributes().InitFromMap(makeAttributes(spanTags))

	makeEvents(ref.Events(), eventNames, eventTimes, eventDroppedTagsCount, eventTags)

	err = makeLinks(ref.Links(), linksLinkedTraceIds, linksLinkedSpanIds, linksTraceStates, linksDroppedTagsCount, linksTags)
	if err != nil {
		return fmt.Errorf("make links: %w", err)
	}

	return nil
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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

func textArraytoString(s pgtype.Text) (*string, error) {
	var d *string
	if err := s.AssignTo(&d); err != nil {
		return nil, fmt.Errorf("assign to: %w", err)
	}
	return d, nil
}
