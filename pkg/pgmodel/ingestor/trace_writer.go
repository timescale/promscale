package ingestor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/model/pdata"
)

const (
	insertSchemaURLSQL = `INSERT INTO %s.schema_url (url) 
		VALUES ($1) RETURNING (id)`
	insertSpanNameSQL = `INSERT INTO %s.span_name (name) 
		VALUES ($1) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING (id)`
	insertInstrumentationLibSQL = `INSERT INTO %s.instrumentation_lib (name, version, schema_url_id) 
		VALUES ($1, $2, $3) RETURNING (id)`
	insertTagKeySQL   = "SELECT %s.put_tag_key($1, $2::%s.tag_type)"
	insertTagSQL      = "SELECT %s.put_tag($1, $2, $3::%s.tag_type)"
	insertSpanLinkSQL = `INSERT INTO %s.link (trace_id, span_id, span_start_time, linked_trace_id, linked_span_id, trace_state, tags, dropped_tags_count, link_nbr) 
		VALUES ($1, $2, $3, $4, $5, $6, %s.get_tag_map($7), $8, $9)`
	insertSpanEventSQL = `INSERT INTO %s.event (time, trace_id, span_id, name, event_nbr, tags, dropped_tags_count) 
		VALUES ($1, $2, $3, $4, $5, %s.get_tag_map($6), $7)`
	insertSpanSQL = `INSERT INTO %s.span (trace_id, span_id, trace_state, parent_span_id, name_id, span_kind, start_time, end_time, span_tags, dropped_tags_count,
		event_time, dropped_events_count, dropped_link_count, status_code, status_message, instrumentation_lib_id, resource_tags, resource_dropped_tags_count, resource_schema_url_id) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, %s.get_tag_map($9), $10, $11, $12, $13, $14, $15, $16, %s.get_tag_map($17), $18, $19)`
)

type traceWriter interface {
	InsertSpanLinks(ctx context.Context, links pdata.SpanLinkSlice, traceID [16]byte, spanID [8]byte, spanStartTime time.Time) error
	InsertSpanEvents(ctx context.Context, events pdata.SpanEventSlice, traceID [16]byte, spanID [8]byte) error
	InsertSpan(ctx context.Context, span pdata.Span, nameID, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error
	InsertSchemaURL(ctx context.Context, sURL string) (id pgtype.Int8, err error)
	InsertSpanName(ctx context.Context, name string) (id pgtype.Int8, err error)
	InsertInstrumentationLibrary(ctx context.Context, name, version, sURL string) (id pgtype.Int8, err error)
}

type traceWriterImpl struct {
	conn pgxconn.PgxConn
}

func newTraceWriter(conn pgxconn.PgxConn) *traceWriterImpl {
	return &traceWriterImpl{
		conn: conn,
	}
}

func (t *traceWriterImpl) InsertSchemaURL(ctx context.Context, sURL string) (id pgtype.Int8, err error) {
	if sURL == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	err = t.conn.QueryRow(ctx, fmt.Sprintf(insertSchemaURLSQL, schema.Trace), sURL).Scan(&id)
	return id, err
}

func (t *traceWriterImpl) InsertSpanName(ctx context.Context, name string) (id pgtype.Int8, err error) {
	if name == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	err = t.conn.QueryRow(ctx, fmt.Sprintf(insertSpanNameSQL, schema.Trace), name).Scan(&id)
	return id, err
}

func (t *traceWriterImpl) InsertInstrumentationLibrary(ctx context.Context, name, version, schemaURL string) (id pgtype.Int8, err error) {
	if name == "" || version == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	var sID pgtype.Int8
	if schemaURL != "" {
		schemaURLID, err := t.InsertSchemaURL(ctx, schemaURL)
		if err != nil {
			return id, err
		}
		err = sID.Set(schemaURLID)
		if err != nil {
			return id, err
		}
	} else {
		sID.Status = pgtype.Null
	}

	err = t.conn.QueryRow(ctx, fmt.Sprintf(insertInstrumentationLibSQL, schema.Trace), name, version, sID).Scan(&id)
	return id, err
}

func (t *traceWriterImpl) insertTags(ctx context.Context, tags map[string]interface{}, typ TagType) error {
	for k, v := range tags {
		_, err := t.conn.Exec(ctx, fmt.Sprintf(insertTagKeySQL, schema.TracePublic, schema.TracePublic), k, typ)

		if err != nil {
			return err
		}

		val, err := json.Marshal(v)
		if err != nil {
			return err
		}

		_, err = t.conn.Exec(ctx, fmt.Sprintf(insertTagSQL, schema.TracePublic, schema.TracePublic),
			k,
			string(val),
			typ,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (t *traceWriterImpl) InsertSpanLinks(ctx context.Context, links pdata.SpanLinkSlice, traceID [16]byte, spanID [8]byte, spanStartTime time.Time) error {
	spanIDInt := ByteArrayToInt64(spanID)
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanIDInt := ByteArrayToInt64(link.SpanID().Bytes())

		rawTags := link.Attributes().AsRaw()
		if err := t.insertTags(ctx, rawTags, LinkTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		_, err = t.conn.Exec(ctx, fmt.Sprintf(insertSpanLinkSQL, schema.Trace, schema.TracePublic),
			TraceIDToUUID(traceID),
			spanIDInt,
			spanStartTime,
			TraceIDToUUID(link.TraceID().Bytes()),
			linkedSpanIDInt,
			getTraceStateValue(link.TraceState()),
			string(jsonTags),
			link.DroppedAttributesCount(),
			i,
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func (t *traceWriterImpl) InsertSpanEvents(ctx context.Context, events pdata.SpanEventSlice, traceID [16]byte, spanID [8]byte) error {
	spanIDInt := ByteArrayToInt64(spanID)
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		rawTags := event.Attributes().AsRaw()
		if err := t.insertTags(ctx, rawTags, EventTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		_, err = t.conn.Exec(ctx, fmt.Sprintf(insertSpanEventSQL, schema.Trace, schema.TracePublic),
			event.Timestamp().AsTime(),
			TraceIDToUUID(traceID),
			spanIDInt,
			event.Name(),
			i,
			string(jsonTags),
			event.DroppedAttributesCount(),
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func (t *traceWriterImpl) InsertSpan(ctx context.Context, span pdata.Span, nameID, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error {
	spanIDInt := ByteArrayToInt64(span.SpanID().Bytes())
	parentSpanIDInt := ByteArrayToInt64(span.ParentSpanID().Bytes())
	rawResourceTags := resourceTags.AsRaw()
	if err := t.insertTags(ctx, rawResourceTags, ResourceTagType); err != nil {
		return err
	}
	jsonResourceTags, err := json.Marshal(rawResourceTags)
	if err != nil {
		return err
	}
	rawTags := span.Attributes().AsRaw()
	if err := t.insertTags(ctx, rawTags, SpanTagType); err != nil {
		return err
	}
	jsonTags, err := json.Marshal(rawTags)
	if err != nil {
		return err
	}

	eventTimeRange := getEventTimeRange(span.Events())

	_, err = t.conn.Exec(
		ctx,
		fmt.Sprintf(insertSpanSQL, schema.Trace, schema.TracePublic, schema.TracePublic),
		TraceIDToUUID(span.TraceID().Bytes()),
		spanIDInt,
		getTraceStateValue(span.TraceState()),
		parentSpanIDInt,
		nameID,
		span.Kind().String(),
		span.StartTimestamp().AsTime(),
		span.EndTimestamp().AsTime(),
		string(jsonTags),
		span.DroppedAttributesCount(),
		eventTimeRange,
		span.DroppedEventsCount(),
		span.DroppedLinksCount(),
		span.Status().Code().String(),
		span.Status().Message(),
		instLibID,
		string(jsonResourceTags),
		0, // TODO: Add resource_dropped_tags_count when it gets exposed upstream.
		rSchemaURLID,
	)

	return err
}

func ByteArrayToInt64(buf [8]byte) int64 {
	return int64(binary.BigEndian.Uint64(buf[:]))
}

func Int64ToByteArray(x int64) [8]byte {
	var res [8]byte
	binary.BigEndian.PutUint64(res[:], uint64(x))
	return res
}

func TraceIDToUUID(buf [16]byte) pgtype.UUID {
	return pgtype.UUID{
		Bytes:  buf,
		Status: pgtype.Present,
	}
}

func getEventTimeRange(events pdata.SpanEventSlice) (result pgtype.Tstzrange) {
	if events.Len() == 0 {
		result.Status = pgtype.Null
		return result
	}

	var lowerTime, upperTime time.Time

	for i := 0; i < events.Len(); i++ {
		eventTime := events.At(i).Timestamp().AsTime()

		if lowerTime.IsZero() || eventTime.Before(lowerTime) {
			lowerTime = eventTime
		}
		if upperTime.IsZero() || eventTime.After(upperTime) {
			upperTime = eventTime
		}
	}

	result = pgtype.Tstzrange{
		Lower:     pgtype.Timestamptz{Time: lowerTime, Status: pgtype.Present},
		Upper:     pgtype.Timestamptz{Time: upperTime, Status: pgtype.Present},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	}

	return result
}

func getTraceStateValue(ts pdata.TraceState) (result pgtype.Text) {
	if string(ts) == "" {
		result.Status = pgtype.Null
	} else {
		result.String = string(ts)
		result.Status = pgtype.Present
	}

	return result
}
