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

func TestSchemaURLBatch(t *testing.T) {
	cache := newSchemaCache()
	incache := schemaURL("incache")
	cache.Insert(incache, pgtype.Int8{Int64: 1337, Valid: true}, incache.SizeInCache())
	cache.Insert(batchItem(schemaURL("invalid")), "foo", 0)

	testCases := []struct {
		name               string
		urls               []string
		expectedBatchQueue int
		queries            []model.SqlQuery
		sendBatchError     error
		expectedError      string
		getIDCheck         func(t *testing.T, batch schemaURLBatch)
	}{
		{
			name:               "happy path",
			urls:               []string{"test", "test", "", "anotherTest", "null", "zero", "incache"},
			expectedBatchQueue: 5,
			queries: []model.SqlQuery{
				{
					Sql:     insertSchemaURLSQL,
					Args:    []interface{}{schemaURL("anotherTest")},
					Results: [][]interface{}{{int64(7)}},
				},
				{
					Sql:     insertSchemaURLSQL,
					Args:    []interface{}{schemaURL("null")},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     insertSchemaURLSQL,
					Args:    []interface{}{schemaURL("test")},
					Results: [][]interface{}{{int64(6)}},
				},
				{
					Sql:     insertSchemaURLSQL,
					Args:    []interface{}{schemaURL("zero")},
					Results: [][]interface{}{{int64(0)}},
				},
			},
			getIDCheck: func(t *testing.T, batch schemaURLBatch) {
				id, err := batch.GetID("test")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 6, Valid: true}, id)

				id, err = batch.GetID("")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{}, id)

				id, err = batch.GetID("nonexistant")
				require.EqualError(t, err, "error getting ID for schema url nonexistant: error getting ID from batch")
				require.Equal(t, pgtype.Int8{}, id)

				id, err = batch.GetID("zero")
				require.EqualError(t, err, "error getting ID for schema url zero: ID is 0")
				require.Equal(t, pgtype.Int8{}, id)

				id, err = batch.GetID("null")
				require.EqualError(t, err, "error getting ID for schema url null: ID is null")
				require.Equal(t, pgtype.Int8{}, id)
			},
		},
		{
			name:               "all urls in cache",
			urls:               []string{"incache"},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch schemaURLBatch) {
				id, err := batch.GetID("incache")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 1337, Valid: true}, id)
			},
		},
		{
			name:               "send batch error",
			urls:               []string{"non-cached url"},
			expectedBatchQueue: 1,
			sendBatchError:     fmt.Errorf("some error"),
			expectedError:      "some error",
		},
		{
			name:               "scan error",
			urls:               []string{"non-cached url"},
			expectedBatchQueue: 1,
			queries: []model.SqlQuery{
				{
					Sql:     insertSchemaURLSQL,
					Args:    []interface{}{schemaURL("non-cached url")},
					Results: [][]interface{}{{"wrong type"}},
				},
			},
			expectedError: `strconv.ParseInt: parsing "wrong type": invalid syntax`,
		},
		{
			name:               "cache error",
			urls:               []string{"invalid"},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch schemaURLBatch) {
				_, err := batch.GetID("invalid")
				require.ErrorIs(t, err, errors.ErrInvalidCacheEntryType)
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			batch := newSchemaUrlBatch(cache)
			require.Len(t, batch.b.batch, 0)

			for _, url := range c.urls {
				batch.Queue(url)
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
