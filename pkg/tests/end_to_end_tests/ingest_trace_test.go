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
	"github.com/timescale/promscale/pkg/jaeger/query"
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

func TestQueryTraces(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		traces := generateTestTrace()
		err = ingestor.IngestTraces(context.Background(), traces)
		require.NoError(t, err)

		q := query.New(pgxconn.NewQueryLoggingPgxConn(db))

		getOperationsTest(t, q)
		findTraceTest(t, q)
		getDependenciesTest(t, q)
	})
}

func getOperationsTest(t testing.TB, q *query.Query) {
	request := spanstore.OperationQueryParameters{
		ServiceName: "service-name-0",
	}
	ops, err := q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "service never existed",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "service-name-0",
		SpanKind:    "SPAN_KIND_UNSPECIFIED",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(ops))

	request = spanstore.OperationQueryParameters{
		ServiceName: "servic-name-0",
		SpanKind:    "SPAN_KIND_CLIENT",
	}
	ops, err = q.GetOperations(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(ops))
}

func findTraceTest(t testing.TB, q *query.Query) {
	request := &spanstore.TraceQueryParameters{
		ServiceName: "service-name-0",
	}

	traces, err := q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 2, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
	}

	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val-not-actually",
		},
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime.Add(time.Second),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime.Add(-time.Second),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime) + (1 * time.Millisecond),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
		DurationMax:  testSpanEndTime.Sub(testSpanStartTime),
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))

	request = &spanstore.TraceQueryParameters{
		ServiceName:   "service-name-0",
		OperationName: "operationA",
		Tags: map[string]string{
			"span-attr": "span-attr-val",
		},
		StartTimeMin: testSpanStartTime,
		StartTimeMax: testSpanStartTime,
		DurationMin:  testSpanEndTime.Sub(testSpanStartTime),
		DurationMax:  testSpanEndTime.Sub(testSpanStartTime) - time.Millisecond,
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 0, len(traces))

	request = &spanstore.TraceQueryParameters{
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
	}
	traces, err = q.FindTraces(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, 1, len(traces))
}

func getDependenciesTest(t testing.TB, q *query.Query) {
	deps, err := q.GetDependencies(context.Background(), testSpanEndTime, 2*testSpanEndTime.Sub(testSpanStartTime))
	require.NoError(t, err)
	require.Equal(t, 1, len(deps))
	require.Equal(t, "service-name-0", deps[0].Parent)
	require.Equal(t, "service-name-1", deps[0].Child)
	require.Equal(t, uint64(4), deps[0].CallCount)
}
