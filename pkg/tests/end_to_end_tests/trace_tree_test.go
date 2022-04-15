// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

/*
We are using a test trace with a tree of spans
with a layout like below:

            1
      /     |     \
   2        3        4
 / | \    / | \     /  \
5  6  7  8  9  10  11  12
   |     |             |
   13    14            15
  /  \                 |
 16  17                18

*/

func TestTraceTreeFuncs(t *testing.T) {
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_trace_tree", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {
		testTraceTreeFuncsInserts(t, ctx, db)
		testTraceTree(t, ctx, db)
		testUpstreamSpans(t, ctx, db)
		testDownstreamSpans(t, ctx, db)
		testSiblingSpans(t, ctx, db)
		testSpanTree(t, ctx, db)
	})
}

func testTraceTreeFuncsInserts(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	var insertSpans = `
WITH x(span_id, parent_span_id) AS
(
    VALUES
        ( 1, null),
        ( 2,    1),
        ( 3,    1),
        ( 4,    1),
        ( 5,    2),
        ( 6,    2),
        ( 7,    2),
        ( 8,    3),
        ( 9,    3),
        (10,    3),
        (11,    4),
        (12,    4),
        (13,    6),
        (14,    8),
        (15,   12),
        (16,   13),
        (17,   13),
        (18,   15)
)
INSERT INTO _ps_trace.span
(
    trace_id,
    span_id,
    parent_span_id,
    operation_id,
    start_time,
	end_time,
	duration_ms,
    span_tags,
    status_code,
    resource_tags,
    resource_schema_url_id
)
SELECT
    '3dadb2bf-0035-433e-b74b-9075cc9260e8',
    x.span_id,
    x.parent_span_id,
    -1,
    now(),
	now(),
	0,
    '{}'::jsonb::tag_map,
    'STATUS_CODE_OK',
    '{}'::jsonb::tag_map,
    -1
FROM x
;`
	exec, err := db.Exec(ctx, insertSpans)
	require.NoError(t, err, "Failed to insert spans")
	require.Equal(t, int64(18), exec.RowsAffected(), "Expected to insert 18 spans. Got: %v", exec.RowsAffected())

	var insertDecoy = `
INSERT INTO _ps_trace.span
(
    trace_id,
    span_id,
    parent_span_id,
    operation_id,
    start_time,
	end_time,
	duration_ms,
    span_tags,
    status_code,
    resource_tags,
    resource_schema_url_id
)
SELECT
    'cac4d4cf-5451-4c1c-b8b6-2e45abe5f87f',
    19,
    null,
    -1,
    now(),
	now(),
	0,
    '{}'::jsonb::tag_map,
    'STATUS_CODE_OK',
    '{}'::jsonb::tag_map,
    -1
;`
	exec, err = db.Exec(ctx, insertDecoy)
	require.NoError(t, err, "Failed to insert decoy span: %v", err)
	require.Equal(t, int64(1), exec.RowsAffected(), "Expected to insert 1 spans. Got: %v", exec.RowsAffected())
}

func testTraceTree(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	const qry = `
	select span_id, coalesce(parent_span_id, -1), lvl, path::text
	from trace_tree('3dadb2bf-0035-433e-b74b-9075cc9260e8') 
	order by span_id`

	type result struct {
		SpanId       int64
		ParentSpanId int64
		Level        int64
		Path         string
	}

	t.Run("trace_tree", func(t *testing.T) {
		expectedResults := []result{
			{1, -1, 1, "{1}"},
			{2, 1, 2, "{1,2}"},
			{3, 1, 2, "{1,3}"},
			{4, 1, 2, "{1,4}"},
			{5, 2, 3, "{1,2,5}"},
			{6, 2, 3, "{1,2,6}"},
			{7, 2, 3, "{1,2,7}"},
			{8, 3, 3, "{1,3,8}"},
			{9, 3, 3, "{1,3,9}"},
			{10, 3, 3, "{1,3,10}"},
			{11, 4, 3, "{1,4,11}"},
			{12, 4, 3, "{1,4,12}"},
			{13, 6, 4, "{1,2,6,13}"},
			{14, 8, 4, "{1,3,8,14}"},
			{15, 12, 4, "{1,4,12,15}"},
			{16, 13, 5, "{1,2,6,13,16}"},
			{17, 13, 5, "{1,2,6,13,17}"},
			{18, 15, 5, "{1,4,12,15,18}"},
		}
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Error running trace_tree: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			require.True(t, rows.Next(), "Expected a row for span id %v but got none", expected.SpanId)

			var actual = result{}
			err = rows.Scan(&actual.SpanId, &actual.ParentSpanId, &actual.Level, &actual.Path)
			require.NoError(t, err, "Failed to scan row. Expected row for span id %v: %v", expected.SpanId, err)

			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "Found more result rows than expected")
	})
}

func testUpstreamSpans(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	type result struct {
		SpanId       int64
		ParentSpanId int64
		Dist         int64
		Path         string
	}

	eval := func(t *testing.T, qry string, expectedResults []result) {
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Error running upstream_spans: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			require.True(t, rows.Next(), "Expected a row for span id %v but got none", expected.SpanId)

			var actual = result{}
			err = rows.Scan(&actual.SpanId, &actual.ParentSpanId, &actual.Dist, &actual.Path)
			require.NoError(t, err, "Failed to scan row. Expected row for span id %v: %v", expected.SpanId, err)

			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "Found more result rows than expected")
	}

	t.Run("upstream_spans", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, path::text
		from upstream_spans('3dadb2bf-0035-433e-b74b-9075cc9260e8', 13)
		order by span_id`
		expectedResults := []result{
			{1, -1, 3, "{1,2,6,13}"},
			{2, 1, 2, "{2,6,13}"},
			{6, 2, 1, "{6,13}"},
			{13, 6, 0, "{13}"},
		}
		eval(t, qry, expectedResults)
	})

	t.Run("upstream_spans_max_dist", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, path::text
		from upstream_spans('3dadb2bf-0035-433e-b74b-9075cc9260e8', 13, 2)
		order by span_id`
		expectedResults := []result{
			{2, 1, 2, "{2,6,13}"},
			{6, 2, 1, "{6,13}"},
			{13, 6, 0, "{13}"},
		}
		eval(t, qry, expectedResults)
	})
}

func testDownstreamSpans(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	type result struct {
		SpanId       int64
		ParentSpanId int64
		Dist         int64
		Path         string
	}

	eval := func(t *testing.T, qry string, expectedResults []result) {
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Error running downstream_spans: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			require.True(t, rows.Next(), "Expected a row for span id %v but got none", expected.SpanId)

			var actual = result{}
			err = rows.Scan(&actual.SpanId, &actual.ParentSpanId, &actual.Dist, &actual.Path)
			require.NoError(t, err, "Failed to scan row. Expected row for span id %v: %v", expected.SpanId, err)

			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "Found more result rows than expected")
	}

	t.Run("downstream_spans", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, path::text
		from downstream_spans('3dadb2bf-0035-433e-b74b-9075cc9260e8', 2) 
		order by span_id`
		expectedResults := []result{
			{2, 1, 0, "{2}"},
			{5, 2, 1, "{2,5}"},
			{6, 2, 1, "{2,6}"},
			{7, 2, 1, "{2,7}"},
			{13, 6, 2, "{2,6,13}"},
			{16, 13, 3, "{2,6,13,16}"},
			{17, 13, 3, "{2,6,13,17}"},
		}
		eval(t, qry, expectedResults)
	})

	t.Run("downstream_spans_max_dist", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, path::text
		from downstream_spans('3dadb2bf-0035-433e-b74b-9075cc9260e8', 2, 2) 
		order by span_id`
		expectedResults := []result{
			{2, 1, 0, "{2}"},
			{5, 2, 1, "{2,5}"},
			{6, 2, 1, "{2,6}"},
			{7, 2, 1, "{2,7}"},
			{13, 6, 2, "{2,6,13}"},
		}
		eval(t, qry, expectedResults)
	})
}

func testSiblingSpans(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	const qry = `
	select span_id, coalesce(parent_span_id, -1)
	from sibling_spans('3dadb2bf-0035-433e-b74b-9075cc9260e8', 8) 
	order by span_id`

	type result struct {
		SpanId       int64
		ParentSpanId int64
	}

	t.Run("sibling_spans", func(t *testing.T) {
		expectedResults := []result{
			{8, 3},
			{9, 3},
			{10, 3},
		}
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Error running sibling_spans: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			if !rows.Next() {
				t.Fatalf("Expected a row for span id %v but got none", expected.SpanId)
			}

			var actual = result{}
			err = rows.Scan(&actual.SpanId, &actual.ParentSpanId)
			require.NoError(t, err, "Failed to scan row. Expected row for span id %v: %v", expected.SpanId, err)

			require.Equal(t, expected, actual)
		}
	})
}

func testSpanTree(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	type result struct {
		SpanId       int64
		ParentSpanId int64
		Dist         int64
		IsUpstream   bool
		IsDownstream bool
		Path         string
	}

	eval := func(t *testing.T, qry string, expectedResults []result) {
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Error running span_tree: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			require.True(t, rows.Next(), "Expected a row for span id %v but got none", expected.SpanId)

			var actual = result{}
			err = rows.Scan(&actual.SpanId, &actual.ParentSpanId, &actual.Dist, &actual.IsUpstream, &actual.IsDownstream, &actual.Path)
			require.NoError(t, err, "Failed to scan row. Expected row for span id %v: %v", expected.SpanId, err)

			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "Found more result rows than expected")
	}

	t.Run("span_tree", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, is_upstream, is_downstream, path::text
		from span_tree('3dadb2bf-0035-433e-b74b-9075cc9260e8', 6) 
		order by span_id`
		expectedResults := []result{
			{1, -1, 2, true, false, "{1,2,6}"},
			{2, 1, 1, true, false, "{2,6}"},
			{6, 2, 0, false, false, "{6}"},
			{13, 6, 1, false, true, "{6,13}"},
			{16, 13, 2, false, true, "{6,13,16}"},
			{17, 13, 2, false, true, "{6,13,17}"},
		}
		eval(t, qry, expectedResults)
	})

	t.Run("span_tree_max_dist", func(t *testing.T) {
		const qry = `
		select span_id, coalesce(parent_span_id, -1), dist, is_upstream, is_downstream, path::text
		from span_tree('3dadb2bf-0035-433e-b74b-9075cc9260e8', 6, 1) 
		order by span_id`
		expectedResults := []result{
			{2, 1, 1, true, false, "{2,6}"},
			{6, 2, 0, false, false, "{6}"},
			{13, 6, 1, false, true, "{6,13}"},
		}
		eval(t, qry, expectedResults)
	})
}
