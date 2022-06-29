// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

type TagType uint

const (
	SpanTagType TagType = 1 << iota
	ResourceTagType
	EventTagType
	LinkTagType
)
const (
	missingServiceName = "OTLPResourceNoServiceName"
	serviceNameTagKey  = "service.name"
)

var (
	linkTableColumns  = []string{"trace_id", "span_id", "span_start_time", "linked_trace_id", "linked_span_id", "trace_state", "tags", "dropped_tags_count", "link_nbr"}
	eventTableColumns = []string{"time", "trace_id", "span_id", "name", "event_nbr", "tags", "dropped_tags_count"}
	spanTableColumns  = []string{"trace_id", "span_id", "parent_span_id", "operation_id", "start_time", "end_time", "duration_ms", "trace_state", "span_tags", "dropped_tags_count",
		"event_time", "dropped_events_count", "dropped_link_count", "status_code", "status_message", "instrumentation_lib_id", "resource_tags", "resource_dropped_tags_count", "resource_schema_url_id"}
)

type Writer interface {
	InsertTraces(ctx context.Context, traces ptrace.Traces) error
}

type traceWriterImpl struct {
	conn pgxconn.PgxConn

	schemaCache  *clockcache.Cache
	instLibCache *clockcache.Cache
	opCache      *clockcache.Cache
	tagCache     *clockcache.Cache
}

func NewWriter(conn pgxconn.PgxConn) *traceWriterImpl {
	return &traceWriterImpl{
		conn:         conn,
		schemaCache:  newSchemaCache(),
		instLibCache: newInstrumentationLibraryCache(),
		opCache:      newOperationCache(),
		tagCache:     newTagCache(),
	}
}

func RegisterTelemetryMetrics(t telemetry.Engine) error {
	err := t.RegisterMetric("promscale_ingested_spans_total", metrics.IngestorItems.With(prometheus.Labels{"type": "trace", "kind": "span", "subsystem": ""}))
	if err != nil {
		return fmt.Errorf("error registering telemetry metric promscale_ingested_spans_total: %w", err)
	}
	return nil
}

func (t *traceWriterImpl) addSpanLinks(linkRows *[][]interface{}, tagsBatch tagBatch, links ptrace.SpanLinkSlice, traceID pgtype.UUID, spanID pgtype.Int8, spanStartTime time.Time) error {
	if spanID.Status != pgtype.Present {
		return fmt.Errorf("spanID must be set")
	}
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanID := getSpanID(link.SpanID().Bytes())
		if linkedSpanID.Status != pgtype.Present {
			return fmt.Errorf("linkedSpanID must be set")
		}

		jsonTags, err := tagsBatch.GetTagMapJSON(link.Attributes().AsRaw(), LinkTagType)
		if err != nil {
			return err
		}
		*linkRows = append(*linkRows, []interface{}{traceID, spanID, spanStartTime, TraceIDToUUID(link.TraceID().Bytes()),
			linkedSpanID, getTraceStateValue(link.TraceState()), jsonTags, link.DroppedAttributesCount(), i})

	}
	return nil
}

func (t *traceWriterImpl) addSpanEvents(eventRows *[][]interface{}, tagsBatch tagBatch, events ptrace.SpanEventSlice, traceID pgtype.UUID, spanID pgtype.Int8) error {
	if spanID.Status != pgtype.Present {
		return fmt.Errorf("spanID must be set")
	}
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		jsonTags, err := tagsBatch.GetTagMapJSON(event.Attributes().AsRaw(), EventTagType)
		if err != nil {
			return err
		}
		*eventRows = append(*eventRows, []interface{}{event.Timestamp().AsTime(), traceID, spanID, event.Name(), i, jsonTags, event.DroppedAttributesCount()})
	}
	return nil
}

func getServiceName(rSpan ptrace.ResourceSpans) string {
	serviceName := missingServiceName
	av, found := rSpan.Resource().Attributes().Get(serviceNameTagKey)
	if found {
		serviceName = av.AsString()
	}
	return serviceName
}

var (
	traceLabel      = prometheus.Labels{"type": "trace"}
	traceSpanLabel  = prometheus.Labels{"type": "trace", "kind": "span"}
	traceEventLabel = prometheus.Labels{"type": "trace", "kind": "event"}
	traceLinkLabel  = prometheus.Labels{"type": "trace", "kind": "link"}
)

var tracesMarshaller = ptrace.NewProtoMarshaler()

func (t *traceWriterImpl) InsertTraces(ctx context.Context, traces ptrace.Traces) error {
	startIngest := time.Now() // Time taken for complete ingestion => Processing + DB insert.
	code := "500"
	metrics.IngestorActiveWriteRequests.With(traceSpanLabel).Inc()
	metrics.IngestorItemsReceived.With(traceSpanLabel).Add(float64(traces.SpanCount()))
	defer func() {
		metrics.IngestorRequests.With(prometheus.Labels{"type": "trace", "code": code}).Inc()
		metrics.IngestorDuration.With(prometheus.Labels{"type": "trace", "code": ""}).Observe(time.Since(startIngest).Seconds())
		metrics.IngestorActiveWriteRequests.With(traceSpanLabel).Dec()
	}()

	rSpans := traces.ResourceSpans()

	sURLBatch := newSchemaUrlBatch(t.schemaCache)
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		url := rSpan.SchemaUrl()
		sURLBatch.Queue(url)

		instLibSpans := rSpan.ScopeSpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			url := instLibSpan.SchemaUrl()
			sURLBatch.Queue(url)
		}
	}
	if err := sURLBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	instrLibBatch := newInstrumentationLibraryBatch(t.instLibCache)
	operationBatch := newOperationBatch(t.opCache)
	tagsBatch := newTagBatch(t.tagCache)
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		serviceName := getServiceName(rSpan)
		instLibSpans := rSpan.ScopeSpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.Scope()

			sURLID, err := sURLBatch.GetID(instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}
			instrLibBatch.Queue(instLib.Name(), instLib.Version(), sURLID)

			spans := instLibSpan.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				spanName := span.Name()
				spanKind, err := getPGKindEnum(span.Kind())
				if err != nil {
					return err
				}

				operationBatch.Queue(serviceName, spanName, spanKind)

				rawResourceTags := rSpan.Resource().Attributes().AsRaw()
				if err := tagsBatch.Queue(rawResourceTags, ResourceTagType); err != nil {
					return err
				}

				rawSpanTags := span.Attributes().AsRaw()
				if err := tagsBatch.Queue(rawSpanTags, SpanTagType); err != nil {
					return err
				}
				for i := 0; i < span.Events().Len(); i++ {
					event := span.Events().At(i)
					rawTags := event.Attributes().AsRaw()
					if err := tagsBatch.Queue(rawTags, EventTagType); err != nil {
						return err
					}
				}

				for i := 0; i < span.Links().Len(); i++ {
					link := span.Links().At(i)

					rawTags := link.Attributes().AsRaw()
					if err := tagsBatch.Queue(rawTags, LinkTagType); err != nil {
						return err
					}
				}

			}
		}
	}

	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "instrumentation_lib"}).Observe(float64(instrLibBatch.b.Len()))
	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "operation"}).Observe(float64(operationBatch.b.Len()))
	metrics.InsertBatchSize.With(prometheus.Labels{"type": "trace", "kind": "tag"}).Observe(float64(tagsBatch.b.Len()))
	if err := instrLibBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := operationBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}
	if err := tagsBatch.SendBatch(ctx, t.conn); err != nil {
		return err
	}

	maxEndTime := time.Time{}
	var (
		spanRows  [][]interface{}
		linkRows  [][]interface{}
		eventRows [][]interface{}
	)
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)
		instLibSpans := rSpan.ScopeSpans()
		serviceName := getServiceName(rSpan)

		url := rSpan.SchemaUrl()
		rSchemaURLID, err := sURLBatch.GetID(url)
		if err != nil {
			return err
		}
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.Scope()

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
				if spanID.Status != pgtype.Present {
					return fmt.Errorf("spanID must be set")
				}
				parentSpanID := getSpanID(span.ParentSpanID().Bytes())
				spanName := span.Name()
				spanKind, err := getPGKindEnum(span.Kind())
				if err != nil {
					return err
				}
				operationID, err := operationBatch.GetID(serviceName, spanName, spanKind)
				if err != nil {
					return err
				}

				if err := t.addSpanEvents(&eventRows, tagsBatch, span.Events(), traceID, spanID); err != nil {
					return err
				}
				if err := t.addSpanLinks(&linkRows, tagsBatch, span.Links(), traceID, spanID, span.StartTimestamp().AsTime()); err != nil {
					return err
				}

				jsonResourceTags, err := tagsBatch.GetTagMapJSON(rSpan.Resource().Attributes().AsRaw(), ResourceTagType)
				if err != nil {
					return err
				}

				jsonTags, err := tagsBatch.GetTagMapJSON(span.Attributes().AsRaw(), SpanTagType)
				if err != nil {
					return err
				}

				eventTimeRange := getEventTimeRange(span.Events())

				// postgresql timestamptz only has microsecond precision while time.Time has nanosecond precision
				start := span.StartTimestamp().AsTime().Truncate(time.Microsecond)
				end := span.EndTimestamp().AsTime().Truncate(time.Microsecond)
				// make sure start <= end
				if end.Before(start) {
					start, end = end, start
				}

				if maxEndTime.Before(end) {
					maxEndTime = end
				}

				statusCode, err := getPGStatusCode(span.Status().Code())
				if err != nil {
					return err
				}

				spanRows = append(spanRows, []interface{}{traceID, spanID, parentSpanID,
					operationID, start, end, float64(end.Sub(start).Nanoseconds()) / float64(1e6), getTraceStateValue(span.TraceState()), jsonTags, span.DroppedAttributesCount(), eventTimeRange, span.DroppedEventsCount(),
					span.DroppedLinksCount(), statusCode, span.Status().Message(), instLibID, jsonResourceTags, 0, rSchemaURLID})

			}
		}
	}

	metrics.InsertBatchSize.With(traceSpanLabel).Observe(float64(len(spanRows)))
	metrics.InsertBatchSize.With(traceEventLabel).Observe(float64(len(eventRows)))
	metrics.InsertBatchSize.With(traceLinkLabel).Observe(float64(len(linkRows)))

	start := time.Now()
	if err := t.insertRows(ctx, "event", eventTableColumns, eventRows); err != nil {
		return fmt.Errorf("error inserting events: %w", err)
	}
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "event"}).Observe(time.Since(start).Seconds())

	start = time.Now()
	if err := t.insertRows(ctx, "link", linkTableColumns, linkRows); err != nil {
		return fmt.Errorf("error inserting links: %w", err)
	}
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "link"}).Observe(time.Since(start).Seconds())

	start = time.Now()
	if err := t.insertRows(ctx, "span", spanTableColumns, spanRows); err != nil {
		return fmt.Errorf("error inserting spans: %w", err)
	}
	metrics.IngestorItems.With(prometheus.Labels{"type": "trace", "kind": "span", "subsystem": ""}).Add(float64(traces.SpanCount()))
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "span"}).Observe(time.Since(start).Seconds())

	code = "2xx"
	metrics.IngestorInsertDuration.With(prometheus.Labels{"type": "trace", "subsystem": "", "kind": "span"}).Observe(time.Since(start).Seconds())
	metrics.IngestorMaxSentTimestamp.With(traceLabel).Set(float64(maxEndTime.UnixNano() / 1e6))

	// Only report telemetry if ingestion successful.
	tput.ReportSpansProcessed(timestamp.FromTime(time.Now()), traces.SpanCount())

	// since otel is making Protobufs internal this is our only chance to get the size of the message
	tracesSizer, ok := tracesMarshaller.(ptrace.Sizer)
	if ok {
		size := tracesSizer.TracesSize(traces)
		metrics.IngestorBytes.With(prometheus.Labels{"type": "trace"}).Add(float64(size))
	}
	return nil
}

func (t *traceWriterImpl) insertRows(ctx context.Context, table string, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}
	conn, err := t.conn.Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if conn != nil {
			conn.Release()
		}
	}()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && tx != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Error(err)
			}
		}
	}()

	tempTableNameRawString := "_trace_temp_" + table
	tempTableName := pgx.Identifier{tempTableNameRawString}

	if _, err = tx.Exec(ctx, "SELECT _ps_trace.ensure_trace_ingest_temp_table($1, $2)", tempTableNameRawString, table); err != nil {
		return err
	}

	if _, err = tx.CopyFrom(ctx, tempTableName, columns, pgx.CopyFromRows(data)); err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO _ps_trace.%[1]s(%[2]s) SELECT %[2]s FROM %[3]s ON CONFLICT DO NOTHING", table, strings.Join(columns, ","), tempTableName.Sanitize())); err != nil {
		return err
	}
	return tx.Commit(ctx)
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

func getEventTimeRange(events ptrace.SpanEventSlice) (result pgtype.Tstzrange) {
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

func getTraceStateValue(ts ptrace.TraceState) (result pgtype.Text) {
	if string(ts) == "" {
		result.Status = pgtype.Null
	} else {
		result.String = string(ts)
		result.Status = pgtype.Present
	}

	return result
}

func getPGStatusCode(pk ptrace.StatusCode) (string, error) {
	switch pk {
	case ptrace.StatusCodeOk:
		return "ok", nil
	case ptrace.StatusCodeError:
		return "error", nil
	case ptrace.StatusCodeUnset:
		return "unset", nil
	default:
		return "", fmt.Errorf("unknown status code: %v", pk)
	}
}
