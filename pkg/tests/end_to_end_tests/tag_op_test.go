// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jackc/pgx/v5/pgxpool"
)

type operator struct {
	operator   string
	returnType string
}

var operators = []operator{
	{
		operator:   "==",
		returnType: "tag_op_equals",
	},
	{
		operator:   "!==",
		returnType: "tag_op_not_equals",
	},
	{
		operator:   "#<",
		returnType: "tag_op_less_than",
	},
	{
		operator:   "#<=",
		returnType: "tag_op_less_than_or_equal",
	},
	{
		operator:   "#>",
		returnType: "tag_op_greater_than",
	},
	{
		operator:   "#>=",
		returnType: "tag_op_greater_than_or_equal",
	},
}

type testValue struct {
	inputType   string
	inputValue  string
	outputType  string
	outputValue string
}

var testValues = []testValue{
	{
		inputType:   "smallint",
		inputValue:  `-1::smallint`,
		outputType:  "number",
		outputValue: `-1`,
	},
	{
		inputType:   "smallint",
		inputValue:  ` 0::smallint`,
		outputType:  "number",
		outputValue: `0`,
	},
	{
		inputType:   "smallint",
		inputValue:  ` 1::smallint`,
		outputType:  "number",
		outputValue: `1`,
	},
	{
		inputType:   "smallint",
		inputValue:  `10::smallint`,
		outputType:  "number",
		outputValue: `10`,
	},
	{
		inputType:   "smallint",
		inputValue:  `99::smallint`,
		outputType:  "number",
		outputValue: `99`,
	},
	{
		inputType:   "int",
		inputValue:  `-1::int`,
		outputType:  "number",
		outputValue: `-1`,
	},
	{
		inputType:   "int",
		inputValue:  ` 0::int`,
		outputType:  "number",
		outputValue: `0`,
	},
	{
		inputType:   "int",
		inputValue:  ` 1::int`,
		outputType:  "number",
		outputValue: `1`,
	},
	{
		inputType:   "int",
		inputValue:  `10::int`,
		outputType:  "number",
		outputValue: `10`,
	},
	{
		inputType:   "int",
		inputValue:  `99::int`,
		outputType:  "number",
		outputValue: `99`,
	},
	{
		inputType:   "bigint",
		inputValue:  `-1::bigint`,
		outputType:  "number",
		outputValue: `-1`,
	},
	{
		inputType:   "bigint",
		inputValue:  ` 0::bigint`,
		outputType:  "number",
		outputValue: `0`,
	},
	{
		inputType:   "bigint",
		inputValue:  ` 1::bigint`,
		outputType:  "number",
		outputValue: `1`,
	},
	{
		inputType:   "bigint",
		inputValue:  `10::bigint`,
		outputType:  "number",
		outputValue: `10`,
	},
	{
		inputType:   "bigint",
		inputValue:  `99::bigint`,
		outputType:  "number",
		outputValue: `99`,
	},
	{
		inputType:   "float4",
		inputValue:  `1.0::float4`,
		outputType:  "number",
		outputValue: `1`,
	},
	{
		inputType:   "float4",
		inputValue:  `1.1::float4`,
		outputType:  "number",
		outputValue: `1.1`,
	},
	{
		inputType:   "float4",
		inputValue:  `1.5::float4`,
		outputType:  "number",
		outputValue: `1.5`,
	},
	{
		inputType:   "float4",
		inputValue:  `1.99::float4`,
		outputType:  "number",
		outputValue: `1.99`,
	},
	{
		inputType:   "float4",
		inputValue:  `'-infinity'::float4`,
		outputType:  "string",
		outputValue: `"-Infinity"`,
	},
	{
		inputType:   "float4",
		inputValue:  `'infinity'::float4`,
		outputType:  "string",
		outputValue: `"Infinity"`,
	},
	{
		inputType:   "float4",
		inputValue:  `'NaN'::float4`,
		outputType:  "string",
		outputValue: `"NaN"`,
	},
	{
		inputType:   "float8",
		inputValue:  `1.0::float8`,
		outputType:  "number",
		outputValue: `1`,
	},
	{
		inputType:   "float8",
		inputValue:  `1.1::float8`,
		outputType:  "number",
		outputValue: `1.1`,
	},
	{
		inputType:   "float8",
		inputValue:  `1.5::float8`,
		outputType:  "number",
		outputValue: `1.5`,
	},
	{
		inputType:   "float8",
		inputValue:  `1.99::float8`,
		outputType:  "number",
		outputValue: `1.99`,
	},
	{
		inputType:   "float8",
		inputValue:  `'-infinity'::float8`,
		outputType:  "string",
		outputValue: `"-Infinity"`,
	},
	{
		inputType:   "float8",
		inputValue:  `'infinity'::float8`,
		outputType:  "string",
		outputValue: `"Infinity"`,
	},
	{
		inputType:   "float8",
		inputValue:  `'NaN'::float8`,
		outputType:  "string",
		outputValue: `"NaN"`,
	},
	{
		inputType:   "numeric",
		inputValue:  `1.0::numeric`,
		outputType:  "number",
		outputValue: `1.0`,
	},
	{
		inputType:   "numeric",
		inputValue:  `1.1::numeric`,
		outputType:  "number",
		outputValue: `1.1`,
	},
	{
		inputType:   "numeric",
		inputValue:  `1.5::numeric`,
		outputType:  "number",
		outputValue: `1.5`,
	},
	{
		inputType:   "numeric",
		inputValue:  `1.99::numeric`,
		outputType:  "number",
		outputValue: `1.99`,
	},
	{
		inputType:   "date",
		inputValue:  `'2021-11-18'::date`,
		outputType:  "string",
		outputValue: `"2021-11-18"`,
	},
	{
		inputType:   "date",
		inputValue:  `'2021-01-01'::date`,
		outputType:  "string",
		outputValue: `"2021-01-01"`,
	},
	{
		inputType:   "date",
		inputValue:  `'2021-12-31'::date`,
		outputType:  "string",
		outputValue: `"2021-12-31"`,
	},
	{
		inputType:   "time",
		inputValue:  `'01:02'::time`,
		outputType:  "string",
		outputValue: `"01:02:00"`,
	},
	{
		inputType:   "time",
		inputValue:  `'12:34:56'::time`,
		outputType:  "string",
		outputValue: `"12:34:56"`,
	},
	{
		inputType:   "time",
		inputValue:  `'23:59:59'::time`,
		outputType:  "string",
		outputValue: `"23:59:59"`,
	},
	{
		inputType:   "timestamptz",
		inputValue:  `'2021-11-18 01:02 UTC'::timestamptz`,
		outputType:  "string",
		outputValue: `"2021-11-18T01:02:00+00:00"`,
	},
	{
		inputType:   "timestamptz",
		inputValue:  `'2021-01-01 12:34:56 UTC'::timestamptz`,
		outputType:  "string",
		outputValue: `"2021-01-01T12:34:56+00:00"`,
	},
	{
		inputType:   "timestamptz",
		inputValue:  `'2021-12-31 23:59:59 UTC'::timestamptz`,
		outputType:  "string",
		outputValue: `"2021-12-31T23:59:59+00:00"`,
	},
	{
		inputType:   "interval",
		inputValue:  `interval '1 second'`,
		outputType:  "string",
		outputValue: `"00:00:01"`,
	},
	{
		inputType:   "interval",
		inputValue:  `interval '1 minute'`,
		outputType:  "string",
		outputValue: `"00:01:00"`,
	},
	{
		inputType:   "interval",
		inputValue:  `interval '1 hour'`,
		outputType:  "string",
		outputValue: `"01:00:00"`,
	},
	{
		inputType:   "text",
		inputValue:  `'hello world'::text`,
		outputType:  "string",
		outputValue: `"hello world"`,
	},
}

func TestTagOpExpressions(t *testing.T) {
	var ctx = context.Background()
	databaseName := fmt.Sprintf("%s_tag_op", *testDatabase)
	withDB(t, databaseName, func(db *pgxpool.Pool, tb testing.TB) {
		testTagOperators(t, db, ctx)
		testTagOpRegexpMatches(t, db, ctx)
		testTagOpRegexpNotMatches(t, db, ctx)
		testTagOpJsonPathExists(t, db, ctx)
	})
}

func testTagOperators(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	const qry = `
select 
    (x.op).tag_key,
    (x.op).value as output_value,
    jsonb_typeof((x.op).value) as output_type,
    pg_typeof(x.op) as return_type
from
(
	select ('tag1' %s %s) as op
) x
`
	for _, opr := range operators {
		for i, tval := range testValues {
			testTagOpExpression(t, db, ctx, qry, opr, tval, i)
		}
	}
}

func testTagOpRegexpMatches(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	const qry = `
select 
    (x.op).tag_key,
    (x.op).value as output_value,
    'text' as output_type,
    pg_typeof(x.op) as return_type
from
(
	select ('tag1' %s %s) as op
) x
`
	opr := operator{
		operator:   "==~",
		returnType: "tag_op_regexp_matches",
	}
	tval := testValue{
		inputType:   "text",
		inputValue:  `'^\\d+$'`,
		outputType:  "text",
		outputValue: `^\\d+$`,
	}
	testTagOpExpression(t, db, ctx, qry, opr, tval, 1)
}

func testTagOpRegexpNotMatches(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	const qry = `
select 
    (x.op).tag_key,
    (x.op).value as output_value,
    'text' as output_type,
    pg_typeof(x.op) as return_type
from
(
	select ('tag1' %s %s) as op
) x
`
	opr := operator{
		operator:   "!=~",
		returnType: "tag_op_regexp_not_matches",
	}
	tval := testValue{
		inputType:   "text",
		inputValue:  `'^\\d+$'`,
		outputType:  "text",
		outputValue: `^\\d+$`,
	}
	testTagOpExpression(t, db, ctx, qry, opr, tval, 1)
}

func testTagOpJsonPathExists(t *testing.T, db *pgxpool.Pool, ctx context.Context) {
	const qry = `
select 
    (x.op).tag_key,
    (x.op).value as output_value,
    'jsonpath' as output_type,
    pg_typeof(x.op) as return_type
from
(
	select ('tag1' %s %s) as op
) x
`
	opr := operator{
		operator:   "@?",
		returnType: "tag_op_jsonb_path_exists",
	}
	tval := testValue{
		inputType:   "jsonpath",
		inputValue:  `'$ ? (@ > 3 && @ < 5)'::jsonpath`,
		outputType:  "jsonpath",
		outputValue: `$?(@ > 3 && @ < 5)`,
	}
	testTagOpExpression(t, db, ctx, qry, opr, tval, 1)
}

func testTagOpExpression(t *testing.T, db *pgxpool.Pool, ctx context.Context, qry string, opr operator, tval testValue, i int) {
	name := fmt.Sprintf("%s_%s_%d", opr.returnType, tval.inputType, i)
	t.Run(name, func(t *testing.T) {
		row := db.QueryRow(ctx, fmt.Sprintf(qry, opr.operator, tval.inputValue))
		var (
			tagKey      string
			outputValue string
			outputType  string
			returnType  string
		)
		err := row.Scan(&tagKey, &outputValue, &outputType, &returnType)
		require.NoError(t, err)
		require.Equal(t, "tag1", tagKey, "Expected the tag keys to be equal: %v != %v", tagKey, "tag1")
		require.Equal(t, tval.outputValue, outputValue, "Expected the output values to be equal: %v != %v", outputValue, tval.outputValue)
		require.Equal(t, tval.outputType, outputType, "Expected the output types to be equal: %v != %v", outputType, tval.outputType)
		require.Equal(t, opr.returnType, returnType, "Expected the return types to be equal: %v != %v", returnType, opr.returnType)
	})
}
