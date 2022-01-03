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
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
)

func TestDeleteSpans(t *testing.T) {
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_delete_all_traces", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {

		// ensure each table has at least one row in it
		_, err := db.Exec(ctx, fmt.Sprintf(`
			insert into %[1]s.schema_url (url) values ('fake.url.com');

			insert into %[1]s.instrumentation_lib (name, version, schema_url_id)
			select 'inst_lib_1', '1.0.0', (select id from %[1]s.schema_url where url = 'fake.url.com' limit 1);

			select %[2]s.put_operation('my.service.name', 'my.span.name', 'SPAN_KIND_UNSPECIFIED');

			select %[2]s.put_tag_key('my.tag.key', 1::ps_trace.tag_type);

			select %[2]s.put_tag('my.tag.key', 'true'::jsonb, 1::ps_trace.tag_type);

			INSERT INTO %[1]s.span
			(
				trace_id, span_id, parent_span_id, operation_id, start_time, end_time, span_tags, status_code,
				resource_tags, resource_schema_url_id
			)
			VALUES
			(
				'3dadb2bf-0035-433e-b74b-9075cc9260e8',
				1234,
				null,
				-1,
				now(),
				now(),
				'{}'::jsonb::tag_map,
				'STATUS_CODE_OK',
				'{}'::jsonb::tag_map,
				-1
			);

			INSERT INTO %[1]s.link
			(
				trace_id, span_id, span_start_time, linked_trace_id, linked_span_id, link_nbr, trace_state, 
				tags, dropped_tags_count
			)
			SELECT
				s.trace_id,
				s.span_id,
				s.start_time,
				s.trace_id,
				s.span_id,
				1,
				'OK',
				'{}'::jsonb::tag_map,
				0
			FROM %[1]s.span s
			;

			INSERT INTO %[1]s.event
			(
				time, trace_id, span_id, event_nbr, name, tags, dropped_tags_count
			)
			SELECT
				now(),
				s.trace_id,
				s.span_id,
				1,
				'my.event',
				'{}'::jsonb::tag_map,
				0
			FROM %[1]s.span s
			;
		`, schema.Trace, schema.TracePublic))
		require.NoError(t, err, "Failed to insert test data.")

		// call the function to delete all the data
		_, err = db.Exec(ctx, fmt.Sprintf("select %s.delete_all_traces()", schema.TracePublic))
		require.NoError(t, err, "delete_all_traces() failed.")

		type result struct {
			Table    string
			Expected int64
		}

		// list the tables in the trace schema
		qry := fmt.Sprintf(`
		select
			tablename,
			case tablename
				when 'tag_key' then 174 -- standard tags should remain
				else 0
			end as expected
		from pg_tables
		where schemaname = '%s'
		`, schema.Trace)
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err, "Failed to list tables in trace schema.")
		results := make([]result, 0)
		for rows.Next() {
			result := result{}
			err := rows.Scan(&result.Table, &result.Expected)
			require.NoError(t, err, "Failed scan results: %v", err)
			results = append(results, result)
		}

		// check all the tables
		for _, r := range results {
			var count int64
			row := db.QueryRow(ctx, fmt.Sprintf("select count(*) from %s.%s", schema.Trace, r.Table))
			err = row.Scan(&count)
			require.NoError(t, err, "Failed to count rows in %s table.", r.Table)
			require.Equalf(t, r.Expected, count, "Failed to empty %s table. %d rows remain.", r.Table, count)
		}
	})
}
