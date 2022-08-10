// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/jaeger/store"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestIngestTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)
	})
}

func TestIngestBrokenTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateBrokenTestTraces()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)
	})
}

func TestIngestTracesMultiTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateTestTraceManyRS()
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
		traces := generateSmallTraces(t)
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
		traces := generateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)

		q := store.New(pgxconn.NewQueryLoggingPgxConn(db), ingestor, &store.DefaultConfig)

		getOperationsTest(t, q)
		findTraceTest(t, q)
		getDependenciesTest(t, q)
	})
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

func findTraceTest(t testing.TB, q *store.Store) {
	for _, tt := range []struct {
		name                  string
		request               *spanstore.TraceQueryParameters
		expectedResponseCount int
	}{
		{
			name: "query by service name",
			request: &spanstore.TraceQueryParameters{
				ServiceName: "service-name-0",
			},
			expectedResponseCount: 2,
		},
		{
			name: "query by service name and operation name",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
			},
			expectedResponseCount: 1,
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
			expectedResponseCount: 1,
		},
		{
			name: "find by event name",
			request: &spanstore.TraceQueryParameters{
				Tags: map[string]string{
					"event": "event",
				},
			},
			expectedResponseCount: 1,
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
			expectedResponseCount: 1,
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
			expectedResponseCount: 0,
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
			expectedResponseCount: 0,
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
			expectedResponseCount: 0,
		},
		{
			name: "find by start time min",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
			},
			expectedResponseCount: 1,
		},
		{
			name: "find by past start time, no result expected",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime.Add(time.Second),
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by start time min and max",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
			},
			expectedResponseCount: 1,
		},
		{
			name: "find by invalid start time, no result expected",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime.Add(-time.Second),
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by duration",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: "find by error = true",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"error": "true",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: "find by no error tag",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "true",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by error=false tag",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "false",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by error=""`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"error": "",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by span kind`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span.kind": "client",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by missing span kind`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"span.kind": "client",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 0,
		},
		{
			name: `find by w3c.tracestate`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"w3c.tracestate": "span-trace-state1",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by missing w3c.tracestate`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationB",
				Tags: map[string]string{
					"w3c.tracestate": "span-trace-state1",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 0,
		},
		{
			name: `find by hostname`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"hostname": "hostname1",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by missing hostname`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationC",
				Tags: map[string]string{
					"hostname": "hostname1",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 0,
		},
		{
			name: `find by jaeger version attribute`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"jaeger.version": "1.0.0",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: `find by missing jaeger version attribute`,
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationC",
				Tags: map[string]string{
					"jaeger.version": "1.0.0",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by invalid timing and duration",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime) + (1 * time.Millisecond),
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by time window and duration_ms",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
				DurationMax:  testSpanEndTime.Sub(testSpanStartTime),
			},
			expectedResponseCount: 1,
		},
		{
			name: "find by time window and duration_ms, no result",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
				DurationMax:  testSpanEndTime.Sub(testSpanStartTime) - time.Millisecond,
			},
			expectedResponseCount: 0,
		},
		{
			name: "find by time and result limit",
			request: &spanstore.TraceQueryParameters{
				ServiceName:   "service-name-0",
				OperationName: "operationA",
				Tags: map[string]string{
					"span-attr": "span-attr-val",
				},
				StartTimeMin: testSpanStartTime,
				StartTimeMax: testSpanStartTime,
				DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
				DurationMax:  testSpanEndTime.Sub(testSpanStartTime),
				NumTraces:    2,
			},
			expectedResponseCount: 1,
		},
	} {
		t.(*testing.T).Run(tt.name, func(t *testing.T) {
			traces, err := q.FindTraces(context.Background(), tt.request)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResponseCount, len(traces))
		})
	}

}

func getDependenciesTest(t testing.TB, q *store.Store) {
	deps, err := q.GetDependencies(context.Background(), testSpanEndTime, 2*testSpanEndTime.Sub(testSpanStartTime))
	require.NoError(t, err)
	require.Equal(t, 1, len(deps))
	require.Equal(t, "service-name-0", deps[0].Parent)
	require.Equal(t, "service-name-1", deps[0].Child)
	require.Equal(t, uint64(4), deps[0].CallCount)
}
