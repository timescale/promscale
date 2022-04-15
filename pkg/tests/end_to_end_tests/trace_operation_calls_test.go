// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestOperationCalls(t *testing.T) {
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_operation_calls", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {
		testOperationCallsInserts(t, ctx, db)
		testOperationCalls(t, ctx, db)
	})
}

func testOperationCalls(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	loc, err := time.LoadLocation("UTC")
	require.NoError(t, err, "Failed to load UTC location")

	type result struct {
		Parent int64
		Child  int64
		Count  int64
	}

	eval := func(t *testing.T, min time.Time, max time.Time, expectedResults []result) {
		const qry = `select parent_operation_id, child_operation_id, cnt from operation_calls($1, $2) order by 1, 2`
		rows, err := db.Query(ctx, qry, min, max)
		require.NoError(t, err, "Error running operation_calls: %v", err)
		defer rows.Close()

		for _, expected := range expectedResults {
			require.True(t, rows.Next(), "Expected a row for parent %d child %d but got none", expected.Parent, expected.Child)

			var actual = result{}
			err = rows.Scan(&actual.Parent, &actual.Child, &actual.Count)
			require.NoError(t, err, "Failed to scan row. Expected row parent %d child %d but got none", expected.Parent, expected.Child, err)

			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "Found more result rows than expected")
	}

	t.Run("before", func(t *testing.T) {
		// there should be no results for 8:59 - 9:00
		min := time.Date(2021, 11, 7, 8, 59, 0, 0, loc)
		max := min.Add(time.Minute)
		expectedResults := make([]result, 0)
		eval(t, min, max, expectedResults)
	})

	t.Run("middle", func(t *testing.T) {
		// there should be 12 results for 9:01 - 9:02
		// one per client->server call
		min := time.Date(2021, 11, 7, 9, 1, 0, 0, loc)
		max := min.Add(time.Minute)
		expectedResults := []result{
			{1, 4, 1},
			{1, 6, 1},
			{1, 8, 1},
			{3, 2, 1},
			{3, 6, 1},
			{3, 8, 1},
			{5, 2, 1},
			{5, 4, 1},
			{5, 8, 1},
			{7, 2, 1},
			{7, 4, 1},
			{7, 6, 1},
		}
		eval(t, min, max, expectedResults)
	})

	t.Run("whole", func(t *testing.T) {
		// there should be 12 results for 9:00 - 9:03
		// 3 per client->server call
		min := time.Date(2021, 11, 7, 9, 0, 0, 0, loc)
		max := min.Add(time.Minute * 3)
		expectedResults := []result{
			{1, 4, 3},
			{1, 6, 3},
			{1, 8, 3},
			{3, 2, 3},
			{3, 6, 3},
			{3, 8, 3},
			{5, 2, 3},
			{5, 4, 3},
			{5, 8, 3},
			{7, 2, 3},
			{7, 4, 3},
			{7, 6, 3},
		}
		eval(t, min, max, expectedResults)
	})

	t.Run("after", func(t *testing.T) {
		// there should be no results for 9:04 - 9:05
		min := time.Date(2021, 11, 7, 9, 4, 0, 0, loc)
		max := min.Add(time.Minute)
		expectedResults := make([]result, 0)
		eval(t, min, max, expectedResults)
	})
}

func testOperationCallsInserts(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	// insert 4 service names
	// insert 2 operations per service - 1 client and 1 server
	// this gives us 8 operations total
	// for each combination of service and operation where one service is a client
	// calling a different service, insert a trace composed of two spans for
	// each minute in a three-minute window
	// this will produce 72 spans (24 per minute for 3 minutes)
	// the sql used to generate the span inserts is in a comment at the bottom of
	// this file

	var insertServiceNames = `
insert into _ps_trace.tag (id, tag_type, key_id, key, value)
overriding system value
values
    (1, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 1'::text)),
    (2, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 2'::text)),
    (3, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 3'::text)),
    (4, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 4'::text))
`
	exec, err := db.Exec(ctx, insertServiceNames)
	require.NoError(t, err, "Failed to insert service names")
	require.Equal(t, int64(4), exec.RowsAffected(), "Expected to insert 4 service names. Got: %v", exec.RowsAffected())

	var insertOperations = `
insert into _ps_trace.operation (id, service_name_id, span_kind, span_name)
overriding system value
values
    (1, 1, 'SPAN_KIND_SERVER'::ps_trace.span_kind, 'service 1 server'),
    (2, 1, 'SPAN_KIND_CLIENT'::ps_trace.span_kind, 'service 1 client'),
    (3, 2, 'SPAN_KIND_SERVER'::ps_trace.span_kind, 'service 2 server'),
    (4, 2, 'SPAN_KIND_CLIENT'::ps_trace.span_kind, 'service 2 client'),
    (5, 3, 'SPAN_KIND_SERVER'::ps_trace.span_kind, 'service 3 server'),
    (6, 3, 'SPAN_KIND_CLIENT'::ps_trace.span_kind, 'service 3 client'),
    (7, 4, 'SPAN_KIND_SERVER'::ps_trace.span_kind, 'service 4 server'),
    (8, 4, 'SPAN_KIND_CLIENT'::ps_trace.span_kind, 'service 4 client')
`
	exec, err = db.Exec(ctx, insertOperations)
	require.NoError(t, err, "Failed to insert operations")
	require.Equal(t, int64(8), exec.RowsAffected(), "Expected to insert 8 operations. Got: %v", exec.RowsAffected())

	var insertSpans = `
insert into _ps_trace.span 
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
select
	x.trace_id::trace_id,
	x.span_id,
	x.parent_span_id,
	x.operation_id,
	x.start_time::timestamptz,
	x.start_time::timestamptz + interval '1 minute',
	60000,
	'{}'::jsonb::tag_map,
	'STATUS_CODE_OK',
	'{}'::jsonb::tag_map,
	-1
from
(
	values
	('9b977f7d-bb0a-4435-bc29-b0c9d78db619', 1, null, 1, '2021-11-07 09:00:01+00'),
	('9b977f7d-bb0a-4435-bc29-b0c9d78db619', 2, 1, 4, '2021-11-07 09:00:01+00'),
	('beae30f8-c6ef-48f0-9794-65a2960ec839', 3, null, 1, '2021-11-07 09:00:01+00'),
	('beae30f8-c6ef-48f0-9794-65a2960ec839', 4, 3, 6, '2021-11-07 09:00:01+00'),
	('4b088016-d300-402a-8b55-de8a800a8c8d', 5, null, 1, '2021-11-07 09:00:01+00'),
	('4b088016-d300-402a-8b55-de8a800a8c8d', 6, 5, 8, '2021-11-07 09:00:01+00'),
	('195228fe-b2d7-4d81-8e90-92fa95ab4d9a', 7, null, 3, '2021-11-07 09:00:01+00'),
	('195228fe-b2d7-4d81-8e90-92fa95ab4d9a', 8, 7, 2, '2021-11-07 09:00:01+00'),
	('87b3a538-fbcc-475d-a8ab-7268127aa25a', 9, null, 3, '2021-11-07 09:00:01+00'),
	('87b3a538-fbcc-475d-a8ab-7268127aa25a', 10, 9, 6, '2021-11-07 09:00:01+00'),
	('ed132e13-2ebe-44e2-89e5-731ad9a1b1c9', 11, null, 3, '2021-11-07 09:00:01+00'),
	('ed132e13-2ebe-44e2-89e5-731ad9a1b1c9', 12, 11, 8, '2021-11-07 09:00:01+00'),
	('39bbc157-e0be-4390-a5fe-b5b7548372b5', 13, null, 5, '2021-11-07 09:00:01+00'),
	('39bbc157-e0be-4390-a5fe-b5b7548372b5', 14, 13, 2, '2021-11-07 09:00:01+00'),
	('e19dfd4f-6a7a-489a-80ee-df58c7af4cdc', 15, null, 5, '2021-11-07 09:00:01+00'),
	('e19dfd4f-6a7a-489a-80ee-df58c7af4cdc', 16, 15, 4, '2021-11-07 09:00:01+00'),
	('fee0ff2e-cfb4-405d-834d-e7e87bdc1942', 17, null, 5, '2021-11-07 09:00:01+00'),
	('fee0ff2e-cfb4-405d-834d-e7e87bdc1942', 18, 17, 8, '2021-11-07 09:00:01+00'),
	('a22a452b-f99e-416a-9545-5e80d6c596b8', 19, null, 7, '2021-11-07 09:00:01+00'),
	('a22a452b-f99e-416a-9545-5e80d6c596b8', 20, 19, 2, '2021-11-07 09:00:01+00'),
	('6ef82cc0-2dca-402b-88a9-fe7fbc82413c', 21, null, 7, '2021-11-07 09:00:01+00'),
	('6ef82cc0-2dca-402b-88a9-fe7fbc82413c', 22, 21, 4, '2021-11-07 09:00:01+00'),
	('6d4293b9-4712-4459-841d-d9dbad103648', 23, null, 7, '2021-11-07 09:00:01+00'),
	('6d4293b9-4712-4459-841d-d9dbad103648', 24, 23, 6, '2021-11-07 09:00:01+00'),
	('e3a3dde8-fbd9-4f65-b964-ff8666d77d0d', 25, null, 1, '2021-11-07 09:01:01+00'),
	('e3a3dde8-fbd9-4f65-b964-ff8666d77d0d', 26, 25, 4, '2021-11-07 09:01:01+00'),
	('570f90b2-8be0-4ec4-85f4-a8a9e5a854ce', 27, null, 1, '2021-11-07 09:01:01+00'),
	('570f90b2-8be0-4ec4-85f4-a8a9e5a854ce', 28, 27, 6, '2021-11-07 09:01:01+00'),
	('5724156d-a18b-4d33-a442-99c42bc4e6cd', 29, null, 1, '2021-11-07 09:01:01+00'),
	('5724156d-a18b-4d33-a442-99c42bc4e6cd', 30, 29, 8, '2021-11-07 09:01:01+00'),
	('8eb2e20d-8343-4022-b92c-5533a47e81f3', 31, null, 3, '2021-11-07 09:01:01+00'),
	('8eb2e20d-8343-4022-b92c-5533a47e81f3', 32, 31, 2, '2021-11-07 09:01:01+00'),
	('d6e1828b-b234-42c1-bcf1-74725ef45ea4', 33, null, 3, '2021-11-07 09:01:01+00'),
	('d6e1828b-b234-42c1-bcf1-74725ef45ea4', 34, 33, 6, '2021-11-07 09:01:01+00'),
	('1eba5df9-160f-45f2-a950-e4b11e69e5a9', 35, null, 3, '2021-11-07 09:01:01+00'),
	('1eba5df9-160f-45f2-a950-e4b11e69e5a9', 36, 35, 8, '2021-11-07 09:01:01+00'),
	('08c67b41-6513-4a1a-879c-22c0a87dfb7b', 37, null, 5, '2021-11-07 09:01:01+00'),
	('08c67b41-6513-4a1a-879c-22c0a87dfb7b', 38, 37, 2, '2021-11-07 09:01:01+00'),
	('4c37e7eb-32ef-4eb5-afc3-0764ef29d82f', 39, null, 5, '2021-11-07 09:01:01+00'),
	('4c37e7eb-32ef-4eb5-afc3-0764ef29d82f', 40, 39, 4, '2021-11-07 09:01:01+00'),
	('8274ddb2-7d7e-468c-a617-31477557e53e', 41, null, 5, '2021-11-07 09:01:01+00'),
	('8274ddb2-7d7e-468c-a617-31477557e53e', 42, 41, 8, '2021-11-07 09:01:01+00'),
	('1331b28a-205c-444c-ae84-834e893d9064', 43, null, 7, '2021-11-07 09:01:01+00'),
	('1331b28a-205c-444c-ae84-834e893d9064', 44, 43, 2, '2021-11-07 09:01:01+00'),
	('d4858640-d98c-4c76-8881-75234ee903ec', 45, null, 7, '2021-11-07 09:01:01+00'),
	('d4858640-d98c-4c76-8881-75234ee903ec', 46, 45, 4, '2021-11-07 09:01:01+00'),
	('d6f4e203-7cef-4b19-8ef9-d1b5309e39e7', 47, null, 7, '2021-11-07 09:01:01+00'),
	('d6f4e203-7cef-4b19-8ef9-d1b5309e39e7', 48, 47, 6, '2021-11-07 09:01:01+00'),
	('ae574eb7-d7b1-40f9-86c4-1b3cabcb4fa8', 49, null, 1, '2021-11-07 09:02:01+00'),
	('ae574eb7-d7b1-40f9-86c4-1b3cabcb4fa8', 50, 49, 4, '2021-11-07 09:02:01+00'),
	('370482cd-158f-4714-9b75-9a04aa87e087', 51, null, 1, '2021-11-07 09:02:01+00'),
	('370482cd-158f-4714-9b75-9a04aa87e087', 52, 51, 6, '2021-11-07 09:02:01+00'),
	('bec9796a-fbc4-4dcb-a77b-597fe1eae792', 53, null, 1, '2021-11-07 09:02:01+00'),
	('bec9796a-fbc4-4dcb-a77b-597fe1eae792', 54, 53, 8, '2021-11-07 09:02:01+00'),
	('164f01d4-6c43-4628-8548-3ee10caf7de0', 55, null, 3, '2021-11-07 09:02:01+00'),
	('164f01d4-6c43-4628-8548-3ee10caf7de0', 56, 55, 2, '2021-11-07 09:02:01+00'),
	('c2ae771d-388e-4624-a6c4-99f1b7ca0f3d', 57, null, 3, '2021-11-07 09:02:01+00'),
	('c2ae771d-388e-4624-a6c4-99f1b7ca0f3d', 58, 57, 6, '2021-11-07 09:02:01+00'),
	('56ba8b54-89c0-4de4-9a9c-1a18532f04f8', 59, null, 3, '2021-11-07 09:02:01+00'),
	('56ba8b54-89c0-4de4-9a9c-1a18532f04f8', 60, 59, 8, '2021-11-07 09:02:01+00'),
	('c88070ad-3f6e-45f0-bf90-c52499083c59', 61, null, 5, '2021-11-07 09:02:01+00'),
	('c88070ad-3f6e-45f0-bf90-c52499083c59', 62, 61, 2, '2021-11-07 09:02:01+00'),
	('ba4929ca-690f-44c8-a0d0-64e0a28a8369', 63, null, 5, '2021-11-07 09:02:01+00'),
	('ba4929ca-690f-44c8-a0d0-64e0a28a8369', 64, 63, 4, '2021-11-07 09:02:01+00'),
	('47fed5aa-4287-46eb-8677-8344420ef8c8', 65, null, 5, '2021-11-07 09:02:01+00'),
	('47fed5aa-4287-46eb-8677-8344420ef8c8', 66, 65, 8, '2021-11-07 09:02:01+00'),
	('12ce962d-c59d-4004-a6a1-73c1dccdadd6', 67, null, 7, '2021-11-07 09:02:01+00'),
	('12ce962d-c59d-4004-a6a1-73c1dccdadd6', 68, 67, 2, '2021-11-07 09:02:01+00'),
	('46dd93ea-f4c0-4db9-b745-a3d755facc78', 69, null, 7, '2021-11-07 09:02:01+00'),
	('46dd93ea-f4c0-4db9-b745-a3d755facc78', 70, 69, 4, '2021-11-07 09:02:01+00'),
	('e7310854-92ad-449c-ae79-8d27b28e77ee', 71, null, 7, '2021-11-07 09:02:01+00'),
	('e7310854-92ad-449c-ae79-8d27b28e77ee', 72, 71, 6, '2021-11-07 09:02:01+00')
) x(trace_id, span_id, parent_span_id, operation_id, start_time)
`
	exec, err = db.Exec(ctx, insertSpans)
	require.NoError(t, err, "Failed to insert spans: %v", err)
	require.Equal(t, int64(72), exec.RowsAffected(), "Expected to insert 72 spans. Got %d", exec.RowsAffected())
}

/*
insert into _ps_trace.tag (id, tag_type, key_id, key, value)
overriding system value
values
    (1, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 1'::text)),
    (2, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 2'::text)),
    (3, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 3'::text)),
    (4, ps_trace.span_tag_type(), 1, 'service.name', to_jsonb('service 4'::text))
;

insert into _ps_trace.operation (id, service_name_id, span_kind, span_name)
overriding system value
values
    (1, 1, 'SPAN_KIND_SERVER'::ps_trace.span_kind,   'service 1 server'),
    (2, 1, 'SPAN_KIND_CLIENT'::ps_trace.span_kind,   'service 1 client'),
    (3, 2, 'SPAN_KIND_SERVER'::ps_trace.span_kind,   'service 2 server'),
    (4, 2, 'SPAN_KIND_CLIENT'::ps_trace.span_kind,   'service 2 client'),
    (5, 3, 'SPAN_KIND_SERVER'::ps_trace.span_kind,   'service 3 server'),
    (6, 3, 'SPAN_KIND_CLIENT'::ps_trace.span_kind,   'service 3 client'),
    (7, 4, 'SPAN_KIND_SERVER'::ps_trace.span_kind,   'service 4 server'),
    (8, 4, 'SPAN_KIND_CLIENT'::ps_trace.span_kind,   'service 4 client')
;

do $block$
declare
    _rec record;
    _span_id bigint := 0;
begin
    for _rec in
    (
        select
            x.server_operation_id,
            x.client_operation_id,
            t as start_minute
        from
        (
            select
                s.id as server_operation_id,
                c.id as client_operation_id
            from _ps_trace.operation s
            cross join _ps_trace.operation c
            where s.span_kind = 'SPAN_KIND_SERVER'
            and c.span_kind = 'SPAN_KIND_CLIENT'
            and s.service_name_id != c.service_name_id
            order by s.id, c.id
        ) x
        cross join generate_series(0, 2) t
    )
    loop
        insert into _ps_trace.span
        (
            trace_id,
            span_id,
            parent_span_id,
            operation_id,
            start_time,
            end_time,
            span_tags,
            status_code,
            resource_tags,
            resource_schema_url_id
        )
        select
            gen_random_uuid(),
            _span_id + 1,
            null,
            _rec.server_operation_id,
            '2021-11-07 09:00:01 UTC'::timestamptz + (_rec.start_minute * interval '1 minute'),
            '2021-11-07 09:01:01 UTC'::timestamptz + (_rec.start_minute * interval '1 minute'),
            '{}'::jsonb::tag_map,
            'STATUS_CODE_OK',
            '{}'::jsonb::tag_map,
            -1
        returning span_id into strict _span_id
        ;

        insert into _ps_trace.span
        (
            trace_id,
            span_id,
            parent_span_id,
            operation_id,
            start_time,
            end_time,
            span_tags,
            status_code,
            resource_tags,
            resource_schema_url_id
        )
        select
            s.trace_id,
            _span_id + 1,
            s.span_id,
            _rec.client_operation_id,
            s.start_time,
            s.end_time,
            s.span_tags,
            s.status_code,
            s.resource_tags,
            s.resource_schema_url_id
        from _ps_trace.span s
        where s.span_id = _span_id
        returning span_id into _span_id
        ;
    end loop;
end;
$block$;

select format($$('%s', %s, %s, %s, '%s'),$$,
    trace_id,
    span_id,
    coalesce(parent_span_id::text, 'null'),
    operation_id,
    start_time
)
from _ps_trace.span
order by span_id
;
*/
