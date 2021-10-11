package ingestor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
	"go.opentelemetry.io/collector/model/pdata"
)

const (
	insertSchemaURLSQL          = `SELECT %s.put_schema_url($1)`
	insertOperationSQL          = `SELECT %s.put_operation($1, $2, $3)`
	insertInstrumentationLibSQL = `SELECT %s.put_instrumentation_lib($1, $2, $3)`
	insertTagKeySQL             = "SELECT %s.put_tag_key($1, $2::%s.tag_type)"
	insertTagSQL                = "SELECT %s.put_tag($1, $2, $3::%s.tag_type)"
	insertSpanLinkSQL           = `INSERT INTO %s.link (trace_id, span_id, span_start_time, linked_trace_id, linked_span_id, trace_state, tags, dropped_tags_count, link_nbr)
		VALUES ($1, $2, $3, $4, $5, $6, %s.get_tag_map($7), $8, $9)`
	insertSpanEventSQL = `INSERT INTO %s.event (time, trace_id, span_id, name, event_nbr, tags, dropped_tags_count)
		VALUES ($1, $2, $3, $4, $5, %s.get_tag_map($6), $7)`
	insertSpanSQL = `INSERT INTO %s.span (trace_id, span_id, trace_state, parent_span_id, operation_id, start_time, end_time, span_tags, dropped_tags_count,
		event_time, dropped_events_count, dropped_link_count, status_code, status_message, instrumentation_lib_id, resource_tags, resource_dropped_tags_count, resource_schema_url_id)
		VALUES ($1, $2, $3, $4, %s.get_operation($5, $6, $7), $8, $9, %s.get_tag_map($10), $11, $12, $13, $14, $15, $16, $17, %s.get_tag_map($18), $19, $20)
		ON CONFLICT DO NOTHING`  // Most cases conflict only happens on retries, safe to ignore duplicate data.
)

type traceWriter interface {
	InsertSpans(ctx context.Context, spans pdata.SpanSlice, serviceName string, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error
	InsertSchemaURL(ctx context.Context, sURL string) (id pgtype.Int8, err error)
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
	err = t.conn.QueryRow(ctx, fmt.Sprintf(insertSchemaURLSQL, schema.TracePublic), sURL).Scan(&id)
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

	err = t.conn.QueryRow(ctx, fmt.Sprintf(insertInstrumentationLibSQL, schema.TracePublic), name, version, sID).Scan(&id)
	return id, err
}

func (t *traceWriterImpl) queueSpanLinks(linkBatch pgxconn.PgxBatch, tagBatch TagBatch, links pdata.SpanLinkSlice, traceID [16]byte, spanID [8]byte, spanStartTime time.Time) error {
	spanIDInt := ByteArrayToInt64(spanID)
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanIDInt := ByteArrayToInt64(link.SpanID().Bytes())

		rawTags := link.Attributes().AsRaw()
		if err := tagBatch.Queue(rawTags, LinkTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		linkBatch.Queue(fmt.Sprintf(insertSpanLinkSQL, schema.Trace, schema.TracePublic),
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
	}
	return nil
}

func (t *traceWriterImpl) queueSpanEvents(eventBatch pgxconn.PgxBatch, tagBatch TagBatch, events pdata.SpanEventSlice, traceID [16]byte, spanID [8]byte) error {
	spanIDInt := ByteArrayToInt64(spanID)
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		rawTags := event.Attributes().AsRaw()
		if err := tagBatch.Queue(rawTags, EventTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		eventBatch.Queue(fmt.Sprintf(insertSpanEventSQL, schema.Trace, schema.TracePublic),
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

func (t *traceWriterImpl) InsertSpans(ctx context.Context, spans pdata.SpanSlice, serviceName string, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error {
	spanBatch := t.conn.NewBatch()
	linkBatch := t.conn.NewBatch()
	eventBatch := t.conn.NewBatch()
	tagBatch := NewTagBatch()
	operationBatch := NewOperationBatch()
	for k := 0; k < spans.Len(); k++ {
		span := spans.At(k)
		traceID := span.TraceID().Bytes()
		spanID := span.SpanID().Bytes()
		spanName := span.Name()
		spanKind := span.Kind().String()

		operationBatch.Queue(Operation{serviceName, spanName, spanKind})

		if err := t.queueSpanEvents(eventBatch, tagBatch, span.Events(), traceID, spanID); err != nil {
			return err
		}
		if err := t.queueSpanLinks(linkBatch, tagBatch, span.Links(), traceID, spanID, span.StartTimestamp().AsTime()); err != nil {
			return err
		}
		spanIDInt := ByteArrayToInt64(spanID)
		parentSpanIDInt := ByteArrayToInt64(span.ParentSpanID().Bytes())
		rawResourceTags := resourceTags.AsRaw()
		if err := tagBatch.Queue(rawResourceTags, ResourceTagType); err != nil {
			return err
		}
		jsonResourceTags, err := json.Marshal(rawResourceTags)
		if err != nil {
			return err
		}
		rawTags := span.Attributes().AsRaw()
		if err := tagBatch.Queue(rawTags, SpanTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}

		eventTimeRange := getEventTimeRange(span.Events())

		spanBatch.Queue(
			fmt.Sprintf(insertSpanSQL, schema.Trace, schema.TracePublic, schema.TracePublic, schema.TracePublic),
			TraceIDToUUID(traceID),
			spanIDInt,
			getTraceStateValue(span.TraceState()),
			parentSpanIDInt,
			serviceName,
			spanName,
			spanKind,
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
	}

	if err := operationBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := tagBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	return t.sendBatches(ctx, eventBatch, linkBatch, spanBatch)

}

func (t *traceWriterImpl) sendBatches(ctx context.Context, batches ...pgxconn.PgxBatch) error {
	for _, batch := range batches {
		br, err := t.conn.SendBatch(ctx, batch)
		if err != nil {
			return err
		}
		if err = br.Close(); err != nil {
			return err
		}
	}
	return nil
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

type Operation struct {
	serviceName string
	spanName    string
	spanKind    string
}

//Operation batch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db
type OperationBatch map[Operation]struct{}

func NewOperationBatch() OperationBatch {
	return make(map[Operation]struct{})
}

func (o OperationBatch) Queue(op Operation) {
	o[op] = struct{}{}
}

func (batch OperationBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	ops := make([]Operation, len(batch))
	i := 0
	for op := range batch {
		ops[i] = op
		i++
	}
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].serviceName == ops[j].serviceName {
			if ops[i].spanName == ops[j].spanName {
				return ops[i].spanKind < ops[j].spanKind
			}
			return ops[i].spanName < ops[j].spanName
		}
		return ops[i].serviceName < ops[j].serviceName
	})

	dbBatch := conn.NewBatch()
	for _, op := range ops {
		dbBatch.Queue(fmt.Sprintf(insertOperationSQL, schema.TracePublic), op.serviceName, op.spanName, op.spanKind)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

type Tag struct {
	key   string
	value string
	typ   TagType
}

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type TagBatch map[Tag]struct{}

func NewTagBatch() TagBatch {
	return make(map[Tag]struct{})
}

func (batch TagBatch) Queue(tags map[string]interface{}, typ TagType) error {
	for k, v := range tags {
		byteVal, err := json.Marshal(v)
		if err != nil {
			return err
		}
		batch[Tag{k, string(byteVal), typ}] = struct{}{}
	}
	return nil
}

func (batch TagBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	tags := make([]Tag, len(batch))
	i := 0
	for op := range batch {
		tags[i] = op
		i++
	}
	sort.Slice(tags, func(i, j int) bool {
		if tags[i].key == tags[j].key {
			if tags[i].value == tags[j].value {
				return tags[i].typ < tags[j].typ
			}
			return tags[i].value < tags[j].value
		}
		return tags[i].key < tags[j].key
	})

	dbBatch := conn.NewBatch()
	for _, tag := range tags {
		dbBatch.Queue(fmt.Sprintf(insertTagKeySQL, schema.TracePublic, schema.TracePublic), tag.key, tag.typ)
		dbBatch.Queue(fmt.Sprintf(insertTagSQL, schema.TracePublic, schema.TracePublic),
			tag.key,
			tag.value,
			tag.typ,
		)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	if err = br.Close(); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
