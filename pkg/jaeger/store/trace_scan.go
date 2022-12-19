// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package store

import (
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jackc/pgx/v5/pgtype"

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
	droppedTagsCounts   int32
	droppedEventsCounts int32
	droppedLinkCounts   int32
	statusCode          string
	statusMessage       pgtype.Text
	traceState          pgtype.Text
	schemaUrl           pgtype.Text
	spanName            string
	resourceTags        []byte
	spanTags            []byte

	// From events table.
	// for events, the entire slice can be nil but not any element within the slice
	eventNames            *[]string
	eventTimes            *[]time.Time
	eventDroppedTagsCount *[]int32
	eventTags             pgtype.FlatArray[[]byte]

	// From instrumentation lib table.
	instLibName      *string
	instLibVersion   *string
	instLibSchemaUrl *string

	// From link table.
	linksLinkedTraceIds   pgtype.FlatArray[pgtype.UUID]
	linksLinkedSpanIds    *[]int64
	linksTraceStates      pgtype.FlatArray[*string]
	linksDroppedTagsCount *[]int32
	linksTags             pgtype.FlatArray[[]byte]
}

func ScanRow(row pgxconn.PgxRows, traces *ptrace.Traces) error {
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

func newMapFromRaw(m map[string]interface{}) pcommon.Map {
	pm := pcommon.NewMap()
	pm.FromRaw(m)
	return pm
}

func populateSpan(
	// From span table.
	resourceSpan ptrace.ResourceSpans,
	dbResult *spanDBResult) error {

	attr, err := makeAttributes(dbResult.resourceTags)
	if err != nil {
		return fmt.Errorf("making resource tags: %w", err)
	}
	newMapFromRaw(attr).CopyTo(resourceSpan.Resource().Attributes())

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
	traceId := makeTraceId(dbResult.traceId)
	ref.SetTraceID(traceId)

	id := makeSpanId(&dbResult.spanId)
	ref.SetSpanID(id)

	// We use a pointer since parent id can be nil. If we use normal int64, we can get parsing errors.
	var temp *int64
	if dbResult.parentSpanId.Valid {
		temp = &dbResult.parentSpanId.Int64
	}
	parentId := makeSpanId(temp)
	ref.SetParentSpanID(parentId)

	if dbResult.traceState.Valid {
		ts := pcommon.NewTraceState()
		ts.FromRaw(dbResult.traceState.String)
		ts.MoveTo(ref.TraceState())
	}

	if dbResult.schemaUrl.Valid {
		resourceSpan.SetSchemaUrl(dbResult.schemaUrl.String)
	}

	ref.SetName(dbResult.spanName)

	if dbResult.kind.Valid {
		ref.SetKind(internalToSpanKind(dbResult.kind.String))
	}

	ref.SetStartTimestamp(pcommon.NewTimestampFromTime(dbResult.startTime))
	ref.SetEndTimestamp(pcommon.NewTimestampFromTime(dbResult.endTime))

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
	newMapFromRaw(attr).CopyTo(ref.Attributes())

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
		ref.Status().SetCode(internalToStatusCode(dbRes.statusCode))
	}
	if dbRes.statusMessage.Valid {
		message := dbRes.statusMessage.String
		ref.Status().SetMessage(message)
	}
	return nil
}

func populateEvents(
	spanEventSlice ptrace.SpanEventSlice,
	dbResult *spanDBResult) error {

	n := len(*dbResult.eventNames)
	for i := 0; i < n; i++ {
		event := spanEventSlice.AppendEmpty()
		event.SetName((*dbResult.eventNames)[i])
		event.SetTimestamp(pcommon.NewTimestampFromTime((*dbResult.eventTimes)[i]))
		event.SetDroppedAttributesCount(uint32((*dbResult.eventDroppedTagsCount)[i]))
		attr, err := makeAttributes(dbResult.eventTags[i])
		if err != nil {
			return fmt.Errorf("making event tags: %w", err)
		}
		newMapFromRaw(attr).CopyTo(event.Attributes())
	}
	return nil
}

func populateLinks(
	spanEventSlice ptrace.SpanLinkSlice,
	dbResult *spanDBResult) error {

	n := len(*dbResult.linksLinkedSpanIds)

	for i := 0; i < n; i++ {
		link := spanEventSlice.AppendEmpty()

		link.SetTraceID(dbResult.linksLinkedTraceIds[i].Bytes)

		spanId := makeSpanId(&(*dbResult.linksLinkedSpanIds)[i])
		link.SetSpanID(spanId)

		if dbResult.linksTraceStates[i] != nil {
			traceState := *(dbResult.linksTraceStates[i])
			ts := pcommon.NewTraceState()
			ts.FromRaw(traceState)
			ts.MoveTo(link.TraceState())
		}
		link.SetDroppedAttributesCount(uint32((*dbResult.linksDroppedTagsCount)[i]))
		attr, err := makeAttributes(dbResult.linksTags[i])
		if err != nil {
			return fmt.Errorf("making link tags: %w", err)
		}
		newMapFromRaw(attr).CopyTo(link.Attributes())
	}
	return nil
}

// makeAttributes makes raw attribute map using tags.
func makeAttributes(tagsJson []byte) (map[string]interface{}, error) {
	if tagsJson == nil {
		return nil, nil
	}
	var tags map[string]interface{}
	if err := json.Unmarshal(tagsJson, &tags); err != nil {
		return map[string]interface{}{}, fmt.Errorf("tags assign to: %w", err)
	}
	tags = sanitizeInt(tags)
	return tags, nil
}

// Hotfix of potential bug: Postgres returns tags as a JSONB map. In this format,
// integers were being returned as float. Hence, this function converts back to integer.
func sanitizeInt(tags map[string]interface{}) map[string]interface{} {
	for k, v := range tags {
		if val, isFloat := v.(float64); isFloat {
			if isIntegral(val) {
				tags[k] = int64(val)
			}
		}
	}
	return tags
}

func isIntegral(val float64) bool {
	return val == float64(int(val))
}

func makeTraceId(s pgtype.UUID) pcommon.TraceID {
	if !s.Valid {
		return pcommon.TraceID{}
	}
	return pcommon.TraceID(s.Bytes)
}

func makeSpanId(s *int64) pcommon.SpanID {
	if s == nil {
		// Send an empty Span ID.
		return pcommon.SpanID{}
	}

	b8 := trace.Int64ToByteArray(*s)
	return pcommon.SpanID(b8)
}
