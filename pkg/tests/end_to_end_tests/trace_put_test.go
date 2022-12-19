// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestTracePuts(t *testing.T) {
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_trace_put", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {
		testPutSchemaURL(t, db, ctx)
		testPutInstrumentationLib(t, db, ctx)
		testPutTagKey(t, db, ctx)
		testPutTag(t, db, ctx)
		testGetTagMap(t, db, ctx)
		testPutOperation(t, db, ctx)
	})
}

func testPutSchemaURL(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testPutSchemaURL", func(t *testing.T) {
		type testVal struct {
			Id  int64
			Url string
		}
		cases := []testVal{
			{1, "this is not a url"},
			{2, "neither is this"},
			{3, "nope not this either"},
		}
		put := func(tv testVal) {
			var actual int64
			// the first iteration should insert a row and return the id
			// the second iteration should return the id of the row
			// inserted on the first iteration
			for i := 0; i < 2; i++ {
				row := db.QueryRow(ctx, "select put_schema_url($1)", tv.Url)
				err := row.Scan(&actual)
				require.NoError(t, err)
				require.Equal(t, tv.Id, actual)
			}
		}
		for _, tv := range cases {
			put(tv)
		}
		rows, err := db.Query(ctx, "select id, url from _ps_trace.schema_url order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range cases {
			require.True(t, rows.Next(), "Fewer rows returned than expected")
			var actual testVal
			err := rows.Scan(&actual.Id, &actual.Url)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "More rows returned than expected")
	})
}

func testPutInstrumentationLib(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testPutInstrumentationLib", func(t *testing.T) {
		type testVal struct {
			Id          int64
			Name        string
			Version     string
			SchemaUrlId int64
		}
		cases := []testVal{
			{1, "inst lib 1", "1.2.3", 1},
			{2, "inst lib 2", "3.2.1", 2},
			{3, "inst lib 3", "2.2.2", 2},
		}
		put := func(tv testVal) {
			var actual int64
			// the first iteration should insert a row and return the id
			// the second iteration should return the id of the row
			// inserted on the first iteration
			for i := 0; i < 2; i++ {
				row := db.QueryRow(ctx, "select put_instrumentation_lib($1, $2, $3)", tv.Name, tv.Version, tv.SchemaUrlId)
				err := row.Scan(&actual)
				require.NoError(t, err)
				require.Equal(t, tv.Id, actual)
			}
		}
		for _, tv := range cases {
			put(tv)
		}
		rows, err := db.Query(ctx, "select id, name, version, schema_url_id from _ps_trace.instrumentation_lib order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range cases {
			require.True(t, rows.Next(), "Fewer rows returned than expected")
			var actual testVal
			err := rows.Scan(&actual.Id, &actual.Name, &actual.Version, &actual.SchemaUrlId)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next(), "More rows returned than expected")
	})
}

func testPutTagKey(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testPutTagKey", func(t *testing.T) {
		put := func(id int64, key string, typ string) {
			var actualId int64
			row := db.QueryRow(ctx, fmt.Sprintf("select put_tag_key($1, %s)", typ), key)
			err := row.Scan(&actualId)
			require.NoError(t, err)
			require.Equal(t, id, actualId, "Unexpected id")
		}
		qry := func(id int64, key string, typ int8) {
			var (
				actualId  int64
				actualKey string
				actualTyp int8
			)
			row := db.QueryRow(ctx, "select id, key, tag_type from _ps_trace.tag_key where id = $1", id)
			err := row.Scan(&actualId, &actualKey, &actualTyp)
			require.NoError(t, err)
			require.Equal(t, id, actualId, "Unexpected id")
			require.Equal(t, key, actualKey, "Unexpected key")
			require.Equal(t, typ, actualTyp, "Unexpected tag type")

		}
		// try to put a standard key which should already exist
		put(1, "service.name", "link_tag_type()")
		// setting a type bit should not create a new row
		put(1, "service.name", "resource_tag_type()")
		qry(1, "service.name", 15)

		// try to put a brand-new key which should not exist
		put(1001, "testing.is.key", "link_tag_type()")
		put(1001, "testing.is.key", "resource_tag_type()")
		qry(1001, "testing.is.key", 10)

		// try to put another brand-new key which should not exist
		put(1002, "key.key.key", "span_tag_type()")
		qry(1002, "key.key.key", 1)
		put(1002, "key.key.key", "event_tag_type()")
		qry(1002, "key.key.key", 5)
	})
}

func testPutTag(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testPutTag", func(t *testing.T) {
		put := func(id int64, key string, value string, typ string) {
			var actualId int64
			row := db.QueryRow(ctx, fmt.Sprintf("select put_tag($1, $2::jsonb, %s)", typ), key, value)
			err := row.Scan(&actualId)
			require.NoError(t, err)
			require.Equal(t, id, actualId, "Unexpected id")
		}
		qry := func(id int64, key string, value string, typ int8) {
			var (
				actualId    int64
				actualKey   string
				actualValue string
				actualTyp   int8
			)
			row := db.QueryRow(ctx, "select id, key, value::text, tag_type from _ps_trace.tag where id = $1", id)
			err := row.Scan(&actualId, &actualKey, &actualValue, &actualTyp)
			require.NoError(t, err)
			require.Equal(t, id, actualId, "Unexpected id")
			require.Equal(t, key, actualKey, "Unexpected key")
			require.Equal(t, value, actualValue, "Unexpected value")
			require.Equal(t, typ, actualTyp, "Unexpected tag type")
		}

		put(1, "service.name", `"super duper service"`, "span_tag_type()")
		qry(1, "service.name", `"super duper service"`, 1)
		put(1, "service.name", `"super duper service"`, "resource_tag_type()")
		qry(1, "service.name", `"super duper service"`, 3)

		put(2, "service.name", `"not so great service"`, "span_tag_type()")
		qry(2, "service.name", `"not so great service"`, 1)
		put(2, "service.name", `"not so great service"`, "resource_tag_type()")
		qry(2, "service.name", `"not so great service"`, 3)

		put(3, "testing.is.key", `42`, "span_tag_type()")
		qry(3, "testing.is.key", `42`, 1)
		put(3, "testing.is.key", `42`, "resource_tag_type()")
		qry(3, "testing.is.key", `42`, 3)

		put(4, "testing.is.key", `true`, "span_tag_type()")
		qry(4, "testing.is.key", `true`, 1)
		put(4, "testing.is.key", `true`, "span_tag_type()")
		qry(4, "testing.is.key", `true`, 1)
	})
}

func testGetTagMap(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testGetTagMap", func(t *testing.T) {
		qry := `
		select *
		from jsonb_each_text(
			get_tag_map($${"service.name": "not so great service", "testing.is.key": 42}$$::jsonb)
		)
		order by key`
		rows, err := db.Query(ctx, qry)
		require.NoError(t, err)
		defer rows.Close()
		var (
			key string
			val string
		)
		require.True(t, rows.Next())
		err = rows.Scan(&key, &val)
		require.NoError(t, err)
		require.Equal(t, "1", key)
		require.Equal(t, "2", val)

		require.True(t, rows.Next())
		err = rows.Scan(&key, &val)
		require.NoError(t, err)
		require.Equal(t, "1001", key)
		require.Equal(t, "3", val)

		require.False(t, rows.Next())
	})
}

func testPutOperation(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	t.Run("testPutOperation", func(t *testing.T) {
		put := func(id int64, serviceName, spanName, spanKind string) {
			var actualId int64
			// multiple puts with the same values should not create more than one row
			for i := 0; i < 2; i++ {
				row := db.QueryRow(ctx, "select put_operation($1, $2, $3)", serviceName, spanName, spanKind)
				err := row.Scan(&actualId)
				require.NoError(t, err)
				require.Equal(t, id, actualId)
			}
		}
		put(1, "super duper service", "span 1", "client")
		put(2, "super duper service", "span 2", "server")
		put(3, "my new service", "span 3", "consumer")
	})
}
