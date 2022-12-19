package trace

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func TestInstrumentationLibraryBatch(t *testing.T) {
	cache := newInstrumentationLibraryCache()
	incache := instrumentationLibrary{"incache", "", pgtype.Int8{Int64: 99, Valid: true}}
	invalid := instrumentationLibrary{"invalid", "", pgtype.Int8{Int64: 10, Valid: true}}
	cache.Insert(incache, pgtype.Int8{Int64: 1337, Valid: true}, incache.SizeInCache())
	cache.Insert(invalid, "foo", 0)

	testCases := []struct {
		name               string
		instLibs           []instrumentationLibrary
		expectedBatchQueue int
		queries            []model.SqlQuery
		sendBatchError     error
		expectedError      string
		getIDCheck         func(t *testing.T, batch instrumentationLibraryBatch)
	}{
		{
			name: "happy path",
			instLibs: []instrumentationLibrary{
				{"", "ignored empty name lib", pgtype.Int8{Int64: 1, Valid: true}},
				{"test", "first", pgtype.Int8{Int64: 2, Valid: true}},
				{"test", "first", pgtype.Int8{Int64: 1, Valid: true}},
				{"test", "first", pgtype.Int8{Int64: 1, Valid: false}},
				{"anotherTest", "second", pgtype.Int8{Int64: 1, Valid: true}},
				{"anotherTest", "first", pgtype.Int8{Int64: 1, Valid: false}},
				{"null", "", pgtype.Int8{Int64: 1, Valid: true}},
				{"zero", "", pgtype.Int8{Int64: 1, Valid: true}},
				{"incache", "", pgtype.Int8{Int64: 99, Valid: true}},
			},
			expectedBatchQueue: 8,
			queries: []model.SqlQuery{
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"anotherTest", "first", pgtype.Int8{Int64: 1, Valid: false}},
					Results: [][]interface{}{{int64(7)}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"anotherTest", "second", pgtype.Int8{Int64: 1, Valid: true}},
					Results: [][]interface{}{{int64(8)}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"null", "", pgtype.Int8{Int64: 1, Valid: true}},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"test", "first", pgtype.Int8{Int64: 1, Valid: false}},
					Results: [][]interface{}{{int64(5)}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"test", "first", pgtype.Int8{Int64: 1, Valid: true}},
					Results: [][]interface{}{{int64(6)}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"test", "first", pgtype.Int8{Int64: 2, Valid: true}},
					Results: [][]interface{}{{int64(6)}},
				},
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"zero", "", pgtype.Int8{Int64: 1, Valid: true}},
					Results: [][]interface{}{{int64(0)}},
				},
			},
			getIDCheck: func(t *testing.T, batch instrumentationLibraryBatch) {
				id, err := batch.GetID("test", "first", pgtype.Int8{Int64: 1, Valid: true})
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 6, Valid: true}, id)

				id, err = batch.GetID("", "missing name", pgtype.Int8{})
				require.NoError(t, err)
				require.Equal(t, pgtype.Int8{Valid: false}, id)

				id, err = batch.GetID("nonexistant", "", pgtype.Int8{})
				require.EqualError(t, err, "error getting ID for instrumentation library {nonexistant  {0 false}}: error getting ID from batch")
				require.Equal(t, pgtype.Int8{Valid: false}, id)

				id, err = batch.GetID("zero", "", pgtype.Int8{Int64: 1, Valid: true})
				require.EqualError(t, err, "error getting ID for instrumentation library {zero  {1 true}}: ID is 0")
				require.Equal(t, pgtype.Int8{Valid: false}, id)

				id, err = batch.GetID("null", "", pgtype.Int8{Int64: 1, Valid: true})
				require.EqualError(t, err, "error getting ID for instrumentation library {null  {1 true}}: ID is null")
				require.Equal(t, pgtype.Int8{Valid: false}, id)
			},
		},
		{
			name:               "all urls in cache",
			instLibs:           []instrumentationLibrary{{"incache", "", pgtype.Int8{Int64: 99, Valid: true}}},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch instrumentationLibraryBatch) {
				id, err := batch.GetID("incache", "", pgtype.Int8{Int64: 99, Valid: true})
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 1337, Valid: true}, id)
			},
		},
		{
			name:               "send batch error",
			instLibs:           []instrumentationLibrary{{"non-cached url", "", pgtype.Int8{Int64: 0, Valid: false}}},
			expectedBatchQueue: 1,
			sendBatchError:     fmt.Errorf("some error"),
			expectedError:      "some error",
		},
		{
			name:               "scan error",
			instLibs:           []instrumentationLibrary{{"non-cached", "", pgtype.Int8{Int64: 0, Valid: false}}},
			expectedBatchQueue: 1,
			queries: []model.SqlQuery{
				{
					Sql:     insertInstrumentationLibSQL,
					Args:    []interface{}{"non-cached", "", pgtype.Int8{Int64: 0, Valid: false}},
					Results: [][]interface{}{{"wrong type"}},
				},
			},
			expectedError: `strconv.ParseInt: parsing "wrong type": invalid syntax`,
		},
		{
			name:               "cache error",
			instLibs:           []instrumentationLibrary{{"invalid", "", pgtype.Int8{Int64: 10, Valid: true}}},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch instrumentationLibraryBatch) {
				_, err := batch.GetID(invalid.name, invalid.version, invalid.schemaURLID)
				require.ErrorIs(t, err, errors.ErrInvalidCacheEntryType)
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			batch := newInstrumentationLibraryBatch(cache)
			require.Len(t, batch.b.batch, 0)

			for _, il := range c.instLibs {
				batch.Queue(il.name, il.version, il.schemaURLID)
			}

			require.Len(t, batch.b.batch, c.expectedBatchQueue)

			conn := model.NewSqlRecorder(c.queries, t)
			if c.sendBatchError != nil {
				conn = model.NewErrorSqlRecorder(c.queries, c.sendBatchError, t)
			}
			err := batch.SendBatch(context.Background(), conn)

			if c.expectedError != "" {
				require.EqualError(t, err, c.expectedError)
				return
			}

			require.NoError(t, err)

			if c.getIDCheck != nil {
				c.getIDCheck(t, batch)
			}
		})
	}
}
