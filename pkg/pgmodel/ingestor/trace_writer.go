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
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	insertSpanEventSQL = `INSERT INTO %s.event (time, trace_id, span_id, name, event_nbr, tags, dropped_tags_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	insertSpanSQL = `INSERT INTO %s.span (trace_id, span_id, trace_state, parent_span_id, operation_id, start_time, end_time, span_tags, dropped_tags_count,
		event_time, dropped_events_count, dropped_link_count, status_code, status_message, instrumentation_lib_id, resource_tags, resource_dropped_tags_count, resource_schema_url_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
		ON CONFLICT DO NOTHING`  // Most cases conflict only happens on retries, safe to ignore duplicate data.
)

type traceWriter interface {
	InsertTraces(ctx context.Context, traces pdata.Traces) error
}

type traceWriterImpl struct {
	conn pgxconn.PgxConn
}

func newTraceWriter(conn pgxconn.PgxConn) *traceWriterImpl {
	return &traceWriterImpl{
		conn: conn,
	}
}

func (t *traceWriterImpl) queueSpanLinks(linkBatch pgxconn.PgxBatch, tagBatch TagBatch, links pdata.SpanLinkSlice, traceID pgtype.UUID, spanID pgtype.Int8, spanStartTime time.Time) error {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanID := getSpanID(link.SpanID().Bytes())

		jsonTags, err := tagBatch.GetTagMapJSON(link.Attributes().AsRaw(), LinkTagType)
		if err != nil {
			return err
		}
		linkBatch.Queue(fmt.Sprintf(insertSpanLinkSQL, schema.Trace),
			traceID,
			spanID,
			spanStartTime,
			TraceIDToUUID(link.TraceID().Bytes()),
			linkedSpanID,
			getTraceStateValue(link.TraceState()),
			string(jsonTags),
			link.DroppedAttributesCount(),
			i,
		)
	}
	return nil
}

func (t *traceWriterImpl) queueSpanEvents(eventBatch pgxconn.PgxBatch, tagBatch TagBatch, events pdata.SpanEventSlice, traceID pgtype.UUID, spanID pgtype.Int8) error {
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		jsonTags, err := tagBatch.GetTagMapJSON(event.Attributes().AsRaw(), EventTagType)
		if err != nil {
			return err
		}
		eventBatch.Queue(fmt.Sprintf(insertSpanEventSQL, schema.Trace),
			event.Timestamp().AsTime(),
			traceID,
			spanID,
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

func getServiceName(rSpan pdata.ResourceSpans) string {
	serviceName := missingServiceName
	av, found := rSpan.Resource().Attributes().Get(serviceNameTagKey)
	if found {
		serviceName = av.AsString()
	}
	return serviceName
}

func (t *traceWriterImpl) InsertTraces(ctx context.Context, traces pdata.Traces) error {
	rSpans := traces.ResourceSpans()

	sURLBatch := NewSchemaUrlBatch()
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		url := rSpan.SchemaUrl()
		sURLBatch.Queue(url)

		instLibSpans := rSpan.InstrumentationLibrarySpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			url := instLibSpan.SchemaUrl()
			sURLBatch.Queue(url)
		}
	}
	if err := sURLBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	instrLibBatch := NewInstrumentationLibraryBatch()
	operationBatch := NewOperationBatch()
	tagBatch := NewTagBatch()
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		serviceName := getServiceName(rSpan)
		instLibSpans := rSpan.InstrumentationLibrarySpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.InstrumentationLibrary()

			sURLID, err := sURLBatch.GetID(instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}
			instrLibBatch.Queue(instLib.Name(), instLib.Version(), sURLID)

			spans := instLibSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				spanName := span.Name()
				spanKind := span.Kind().String()

				operationBatch.Queue(serviceName, spanName, spanKind)

				rawResourceTags := rSpan.Resource().Attributes().AsRaw()
				if err := tagBatch.Queue(rawResourceTags, ResourceTagType); err != nil {
					return err
				}

				rawSpanTags := span.Attributes().AsRaw()
				if err := tagBatch.Queue(rawSpanTags, SpanTagType); err != nil {
					return err
				}
				for i := 0; i < span.Events().Len(); i++ {
					event := span.Events().At(i)
					rawTags := event.Attributes().AsRaw()
					if err := tagBatch.Queue(rawTags, EventTagType); err != nil {
						return err
					}
				}

				for i := 0; i < span.Links().Len(); i++ {
					link := span.Links().At(i)

					rawTags := link.Attributes().AsRaw()
					if err := tagBatch.Queue(rawTags, LinkTagType); err != nil {
						return err
					}
				}

			}
		}
	}
	if err := instrLibBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := operationBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := tagBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	spanBatch := t.conn.NewBatch()
	linkBatch := t.conn.NewBatch()
	eventBatch := t.conn.NewBatch()
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		instLibSpans := rSpan.InstrumentationLibrarySpans()
		serviceName := getServiceName(rSpan)

		url := rSpan.SchemaUrl()
		rSchemaURLID, err := sURLBatch.GetID(url)
		if err != nil {
			return err
		}
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.InstrumentationLibrary()

			sURLID, err := sURLBatch.GetID(instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}
			instLibID, err := instrLibBatch.GetID(instLib.Name(), instLib.Version(), sURLID)
			if err != nil {
				return err
			}

			spans := instLibSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				traceID := TraceIDToUUID(span.TraceID().Bytes())
				spanID := getSpanID(span.SpanID().Bytes())
				parentSpanID := getSpanID(span.ParentSpanID().Bytes())
				spanName := span.Name()
				spanKind := span.Kind().String()
				operationID, err := operationBatch.GetID(serviceName, spanName, spanKind)
				if err != nil {
					return err
				}

				if err := t.queueSpanEvents(eventBatch, tagBatch, span.Events(), traceID, spanID); err != nil {
					return err
				}
				if err := t.queueSpanLinks(linkBatch, tagBatch, span.Links(), traceID, spanID, span.StartTimestamp().AsTime()); err != nil {
					return err
				}

				jsonResourceTags, err := tagBatch.GetTagMapJSON(rSpan.Resource().Attributes().AsRaw(), ResourceTagType)
				if err != nil {
					return err
				}

				jsonTags, err := tagBatch.GetTagMapJSON(span.Attributes().AsRaw(), SpanTagType)
				if err != nil {
					return err
				}

				eventTimeRange := getEventTimeRange(span.Events())

				spanBatch.Queue(
					fmt.Sprintf(insertSpanSQL, schema.Trace),
					traceID,
					spanID,
					getTraceStateValue(span.TraceState()),
					parentSpanID,
					operationID,
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
		}
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

func getSpanID(buf [8]byte) pgtype.Int8 {
	i := int64(binary.BigEndian.Uint64(buf[:]))

	if i != 0 {
		return pgtype.Int8{
			Int:    i,
			Status: pgtype.Present,
		}
	}

	return pgtype.Int8{
		Status: pgtype.Null,
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
type OperationBatch map[Operation]int64

func NewOperationBatch() OperationBatch {
	return make(map[Operation]int64)
}

func (o OperationBatch) Queue(serviceName, spanName, spanKind string) {
	o[Operation{serviceName, spanName, spanKind}] = 0
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
	for _, op := range ops {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[op] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}
func (batch OperationBatch) GetID(serviceName, spanName, spanKind string) (pgtype.Int8, error) {
	id, ok := batch[Operation{serviceName, spanName, spanKind}]
	if id == 0 || !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("operation id not found: %s %s %s", serviceName, spanName, spanKind)
	}
	return pgtype.Int8{Int: id, Status: pgtype.Present}, nil
}

type Tag struct {
	key   string
	value string
	typ   TagType
}

type TagIDs struct {
	keyID   int64
	valueID int64
}

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type TagBatch map[Tag]TagIDs

func NewTagBatch() TagBatch {
	return make(map[Tag]TagIDs)
}

func (batch TagBatch) Queue(tags map[string]interface{}, typ TagType) error {
	for k, v := range tags {
		byteVal, err := json.Marshal(v)
		if err != nil {
			return err
		}
		batch[Tag{k, string(byteVal), typ}] = TagIDs{}
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
	for _, tag := range tags {
		var keyID int64
		var valueID int64
		if err := br.QueryRow().Scan(&keyID); err != nil {
			return err
		}
		if err := br.QueryRow().Scan(&valueID); err != nil {
			return err
		}
		batch[tag] = TagIDs{keyID: keyID, valueID: valueID}
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func (batch TagBatch) GetTagMapJSON(tags map[string]interface{}, typ TagType) ([]byte, error) {
	tagMap := make(map[int64]int64)
	for k, v := range tags {
		byteVal, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		ids, ok := batch[Tag{k, string(byteVal), typ}]
		if !ok || ids.keyID == 0 || ids.valueID == 0 {
			return nil, fmt.Errorf("tag id not found: %s %v(rendered as %s) %v", k, v, string(byteVal), typ)

		}
		tagMap[ids.keyID] = ids.valueID
	}

	jsonBytes, err := json.Marshal(tagMap)
	if err != nil {
		return nil, err
	}
	return jsonBytes, nil
}

type SchemaUrl string

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type SchemaUrlBatch map[SchemaUrl]int64

func NewSchemaUrlBatch() SchemaUrlBatch {
	return make(map[SchemaUrl]int64)
}

func (batch SchemaUrlBatch) Queue(url string) {
	if url != "" {
		batch[SchemaUrl(url)] = 0
	}
}

func (batch SchemaUrlBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	urls := make([]SchemaUrl, len(batch))
	i := 0
	for url := range batch {
		urls[i] = url
		i++
	}
	sort.Slice(urls, func(i, j int) bool {
		return urls[i] < urls[j]
	})

	dbBatch := conn.NewBatch()
	for _, sURL := range urls {
		dbBatch.Queue(fmt.Sprintf(insertSchemaURLSQL, schema.TracePublic), sURL)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	for _, sURL := range urls {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[sURL] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func (batch SchemaUrlBatch) GetID(url string) (pgtype.Int8, error) {
	if url == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, ok := batch[SchemaUrl(url)]
	if id == 0 || !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("schema url id not found")
	}
	return pgtype.Int8{Int: id, Status: pgtype.Present}, nil
}

type InstrumentationLibrary struct {
	name        string
	version     string
	SchemaUrlID pgtype.Int8
}

//TagBatch queues up items to send to the db but it sorts before sending
//this avoids deadlocks in the db. It also avoids sending the same tags repeatedly.
type InstrumentationLibraryBatch map[InstrumentationLibrary]int64

func NewInstrumentationLibraryBatch() InstrumentationLibraryBatch {
	return make(map[InstrumentationLibrary]int64)
}

func (batch InstrumentationLibraryBatch) Queue(name, version string, schemaUrlID pgtype.Int8) {
	if name != "" {
		batch[InstrumentationLibrary{name, version, schemaUrlID}] = 0
	}
}

func (batch InstrumentationLibraryBatch) SendBatch(ctx context.Context, conn pgxconn.PgxConn) error {
	libs := make([]InstrumentationLibrary, len(batch))
	i := 0
	for lib := range batch {
		libs[i] = lib
		i++
	}
	sort.Slice(libs, func(i, j int) bool {
		if libs[i].name == libs[j].name {
			if libs[i].version == libs[j].version {
				if libs[i].SchemaUrlID.Status == libs[j].SchemaUrlID.Status {
					return libs[i].SchemaUrlID.Int < libs[j].SchemaUrlID.Int
				}
				return libs[i].SchemaUrlID.Status < libs[j].SchemaUrlID.Status
			}
			return libs[i].version < libs[j].version
		}
		return libs[i].version < libs[j].version
	})

	dbBatch := conn.NewBatch()
	for _, lib := range libs {
		dbBatch.Queue(fmt.Sprintf(insertInstrumentationLibSQL, schema.TracePublic), lib.name, lib.version, lib.SchemaUrlID)
	}

	br, err := conn.SendBatch(ctx, dbBatch)
	if err != nil {
		return err
	}
	for _, lib := range libs {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			return err
		}
		batch[lib] = id
	}
	if err = br.Close(); err != nil {
		return err
	}
	return nil
}

func (batch InstrumentationLibraryBatch) GetID(name, version string, schemaUrlID pgtype.Int8) (pgtype.Int8, error) {
	if name == "" {
		return pgtype.Int8{Status: pgtype.Null}, nil
	}
	id, ok := batch[InstrumentationLibrary{name, version, schemaUrlID}]
	if id == 0 || !ok {
		return pgtype.Int8{Status: pgtype.Null}, fmt.Errorf("instrumention library id not found: %s %s", name, version)
	}
	return pgtype.Int8{Int: id, Status: pgtype.Present}, nil
}
