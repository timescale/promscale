// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jaegertracing/jaeger/model"
	jaeger_integration_tests "github.com/jaegertracing/jaeger/plugin/storage/integration"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spyzhov/ajson"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/jaeger/store"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tests/testdata"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestIngestTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := testdata.GenerateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)
	})
}

func TestIngestBrokenTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := testdata.GenerateBrokenTestTraces()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)
	})
}

func TestIngestTracesMultiTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := testdata.GenerateTestTraceManyRS()
		for traceIndex := 0; traceIndex < len(traces); traceIndex++ {
			err = ingestor.IngestTraces(context.Background(), traces[traceIndex])
		}
		require.NoError(t, err)
	})
}

func TestIngestTracesFromDataset(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := testdata.LoadSmallTraces(t)
		for i := range traces {
			err = ingestor.IngestTraces(context.Background(), traces[i])
			require.NoError(t, err)
		}
	})
}

func TestQueryTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		fixtures, err := getTracesFixtures()
		require.NoError(t, err)
		err = ingestor.IngestTraces(context.Background(), fixtures.traces)
		require.NoError(t, err)

		q := store.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		getOperationsTest(t, q)
		findTraceTest(t, q, fixtures)
		getDependenciesTest(t, q)
		findTracePlanTest(t, q, db)
	})
}

func findTracePlanTest(t testing.TB, q *store.Store, db *pgxpool.Pool) {
	c, err := db.Acquire(context.Background())
	require.NoError(t, err)
	defer c.Release()

	request := &spanstore.TraceQueryParameters{
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		NumTraces: 2,
	}
	tInfo, err := store.FindTagInfo(context.Background(), request, pgxconn.NewPgxConn(db))
	require.NoError(t, err)
	qry, params := q.GetBuilder().BuildTraceIDSubquery(request, tInfo)

	//testing that specific query plans are /possible/ not executed due to planner statistics
	_, err = c.Exec(context.Background(), "SET enable_seqscan=0")
	require.NoError(t, err)
	_, err = c.Exec(context.Background(), "SET enable_indexscan=0")
	require.NoError(t, err)
	res := ""
	err = c.QueryRow(context.Background(), "EXPLAIN(ANALYZE, VERBOSE, FORMAT JSON) "+qry, params...).Scan(&res)
	require.NoError(t, err)

	//span tags use index
	match, err := ajson.JSONPath([]byte(res), `$..Plans[?(@."Index Cond" =~ '\\(s.span_tags\\)::jsonb @> ANY \\(.*\\)')]`)
	require.NoError(t, err)
	require.NotEmpty(t, match)

	//qry event tags now
	request = &spanstore.TraceQueryParameters{
		Tags: map[string]string{
			"span-event-attr": "span-event-attr-val",
		},
		NumTraces: 2,
	}
	tInfo, err = store.FindTagInfo(context.Background(), request, pgxconn.NewPgxConn(db))
	require.NoError(t, err)
	qry, params = q.GetBuilder().BuildTraceIDSubquery(request, tInfo)
	err = c.QueryRow(context.Background(), "EXPLAIN(ANALYZE, VERBOSE, FORMAT JSON) "+qry, params...).Scan(&res)
	require.NoError(t, err)

	//event tags use index
	match, err = ajson.JSONPath([]byte(res), `$..Plans[?(@."Index Cond" =~ '\\(e.tags\\)::jsonb @> ANY \\(.*\\)')]`)
	require.NoError(t, err)
	require.NotEmpty(t, match)

	/*rows, err := c.Query(context.Background(), "EXPLAIN (ANALYZE, VERBOSE) "+qry, params...)
	require.NoError(t, err)
	for rows.Next() {
		rows.Scan(&res)
		fmt.Println(res)
	}*/
}

func getOperationsTest(t testing.TB, q *store.Store) {
	for _, tt := range []struct {
		name                  string
		request               spanstore.OperationQueryParameters
		expectedResponseCount int
	}{
		{
			name: "find operation by service name",
			request: spanstore.OperationQueryParameters{
				ServiceName: "service-name-0",
			},
			expectedResponseCount: 2,
		},
		{
			name: "find operation by non existent service name",
			request: spanstore.OperationQueryParameters{
				ServiceName: "service never existed",
			},
			expectedResponseCount: 0,
		},
		{
			name: "find operation by SPAN_KIND_UNSPECIFIED",
			request: spanstore.OperationQueryParameters{
				ServiceName: "service-name-0",
				SpanKind:    "SPAN_KIND_UNSPECIFIED",
			},
			expectedResponseCount: 1,
		},
		{
			name: "find operation by SPAN_KIND_CLIENT",
			request: spanstore.OperationQueryParameters{
				ServiceName: "service-name-0",
				SpanKind:    "SPAN_KIND_CLIENT",
			},
			expectedResponseCount: 1,
		},
	} {
		t.(*testing.T).Run(tt.name, func(t *testing.T) {
			ops, err := q.GetOperations(context.Background(), tt.request)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResponseCount, len(ops))
		})
	}
}

// tracesFixtures contains `traces` to be used as a fixture in both OTEL
// ptrace.Trace and Jager []*model.Batch representation. Also, each of
// the traces is present in the Jaeger model.Traces format so that they
// can be used to compare results from Jaeger query responses.
type tracesFixtures struct {
	traces  ptrace.Traces
	batches []*model.Batch
	trace1  *model.Trace
	trace2  *model.Trace
}

func tracesFixturesToBatches(traces ptrace.Traces) ([]*model.Batch, error) {
	batches, err := store.ProtoFromTraces(traces)
	if err != nil {
		return nil, err
	}
	for _, b := range batches {
		for _, s := range b.Spans {
			// ProtoFromTraces doesn't populates span.Process because it is already been exposed by batch.Process.
			// See https://github.com/jaegertracing/jaeger-idl/blob/05fe64e9c305526901f70ff692030b388787e388/proto/api_v2/model.proto#L152-L160
			//
			// Reusing the same reference makes the diff lib think is a cyclic
			// reference and returns an error.
			s.Process = &model.Process{
				ServiceName: b.Process.ServiceName,
				Tags:        append([]model.KeyValue{}, b.Process.Tags...),
			}
		}
	}
	return batches, nil
}

func getTracesFixtures() (tracesFixtures, error) {

	traces := testdata.GenerateTestTrace()
	batches, err := tracesFixturesToBatches(traces)

	if err != nil {
		return tracesFixtures{}, err
	}

	// After passing the traces from testdata.GenerateTestTrace through the
	// translator we end up with 2 batches. The first one has 8 spans, the second
	// 4. The first 4 spans of the first batch belong to the same trace and the
	// other 4 belong to the same trace as all the spans in the second batch,
	// meaning that there are 2 traces.
	//
	// Batches are ordered by Process, the analogous to Resource in OTEL.
	//
	// Basically::
	//   batches = [
	//     [sT1, sT1, sT1, sT1, s1T2, s1T2, s1T2, s1T2],
	//     [s2T2, s2T2, s2T2, s2T2],
	//   ]
	//
	// Note: in the example there are just three types of spans sT1, s1T2 and
	// s2T2, that's because each type shares the same attributes they just have
	// unique Span IDs.
	trace1 := &model.Trace{
		Spans:      batches[0].Spans[:4],
		ProcessMap: nil,
		Warnings:   make([]string, 0),
	}

	trace2Spans := make([]*model.Span, 0)
	trace2Spans = append(trace2Spans, batches[0].Spans[4:]...)
	trace2Spans = append(trace2Spans, batches[1].Spans...)

	trace2 := &model.Trace{
		Spans:      trace2Spans,
		ProcessMap: nil,
		Warnings:   make([]string, 0),
	}

	// We recreate the batches to have a unique copy that can be
	// modified without altering trace1 and trace2
	batches, err = tracesFixturesToBatches(traces.Clone())
	if err != nil {
		return tracesFixtures{}, err
	}

	return tracesFixtures{
		traces,
		batches,
		trace1,
		trace2,
	}, nil
}

func findTraceTest(t testing.TB, q *store.Store, fixtures tracesFixtures) {
	trace1 := fixtures.trace1
	trace2 := fixtures.trace2

	for _, tt := range []struct {
		name           string
		request        *spanstore.TraceQueryParameters
		expectedTraces []*model.Trace
	}{
		{
			name: "query by service name",
			request: &spanstore.TraceQueryParameters{
				ServiceName: "service-name-0",
			},
			expectedTraces: []*model.Trace{trace1, trace2},
		},
		{
			name: "query by service name and operation name",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by service name, operation and tags",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by event name",
			request: &spanstore.TraceQueryParameters{
				Tags: map[string]string{
					"event": "event",
				},
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by both span and event attr",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-event-attr": "span-event-attr-val",
					"span-attr":       "span-attr-val",
				},
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by event tag, service and operation name",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-event-attr": "span-event-attr-val",
				},
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by event name and non existent tag, service and operation",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"event":           "event-with-attr",
					"span-event-attr": "not-exist",
				},
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by non existent event name",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"event": "not-exists",
				},
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by non existent span tag",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val-not-actually",
				},
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by start time min",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by past start time, no result expected",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime.Add(time.Second),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by start time min and max",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by invalid start time, no result expected",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime.Add(-time.Second),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by duration",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by error = true",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"error": "true",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by no error tag",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "true",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by error=false tag",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "false",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace2},
		},
		{
			name: `find by error=""`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace2},
		},
		{
			name: `find by span kind`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span.kind": "client",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: `find by missing span kind`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"span.kind": "client",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: `find by w3c.tracestate`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"w3c.tracestate": "span-trace-state1",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: `find by missing w3c.tracestate`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"w3c.tracestate": "span-trace-state1",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: `find by hostname`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"hostname": "hostname1",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: `find by missing hostname`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationC",
				Tags: map[string]string{
					"hostname": "hostname1",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: `find by jaeger version attribute`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"jaeger.version": "1.0.0",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: `find by missing jaeger version attribute`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationC",
				Tags: map[string]string{
					"jaeger.version": "1.0.0",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by invalid timing and duration",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime) + (1 * time.Millisecond),
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by time window and duration_ms",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
				DurationMax:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
			},
			expectedTraces: []*model.Trace{trace1},
		},
		{
			name: "find by time window and duration_ms, no result",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
				DurationMax:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime) - time.Millisecond,
			},
			expectedTraces: []*model.Trace{},
		},
		{
			name: "find by time and result limit",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testdata.TestSpanStartTime,
				StartTimeMax: testdata.TestSpanStartTime,
				DurationMin:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
				DurationMax:  testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime),
				NumTraces:    2,
			},
			expectedTraces: []*model.Trace{trace1},
		},
	} {
		t.(*testing.T).Run(tt.name, func(t *testing.T) {
			traces, err := q.FindTraces(context.Background(), tt.request)
			require.NoError(t, err)
			jaeger_integration_tests.CompareSliceOfTraces(t, tt.expectedTraces, traces)
		})
	}

}

func getDependenciesTest(t testing.TB, q *store.Store) {
	deps, err := q.GetDependencies(context.Background(), testdata.TestSpanEndTime, 2*testdata.TestSpanEndTime.Sub(testdata.TestSpanStartTime))
	require.NoError(t, err)
	require.Equal(t, 1, len(deps))
	require.Equal(t, "service-name-0", deps[0].Parent)
	require.Equal(t, "service-name-1", deps[0].Child)
	require.Equal(t, uint64(4), deps[0].CallCount)
}
