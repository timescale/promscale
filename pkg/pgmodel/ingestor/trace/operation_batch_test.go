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

const (
	defaultServiceName = "serviceName"
	defaultSpanKind    = "spanKind"
)

func TestOperationBatch(t *testing.T) {
	cache := newOperationCache()
	incache := operation{"", "incache", ""}
	invalid := operation{"", "invalid", ""}
	cache.Insert(incache, pgtype.Int8{Int64: 1337, Valid: true}, incache.SizeInCache())
	cache.Insert(invalid, "foo", 0)

	testCases := []struct {
		name               string
		operations         []operation
		expectedBatchQueue int
		queries            []model.SqlQuery
		sendBatchError     error
		expectedError      string
		getIDCheck         func(t *testing.T, batch operationBatch)
	}{
		{
			name: "happy path",
			operations: []operation{
				{"second", "test", ""},
				{"first", "test", ""},
				{"", "anotherTest", "second"},
				{"", "anotherTest", "first"},
				{"", "null", ""},
				{"", "zero", ""},
				{"", "incache", ""},
			},
			expectedBatchQueue: 7,
			queries: []model.SqlQuery{
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"", "anotherTest", "first"},
					Results: [][]interface{}{{int64(7)}},
				},
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"", "anotherTest", "second"},
					Results: [][]interface{}{{int64(8)}},
				},
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"", "null", ""},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"", "zero", ""},
					Results: [][]interface{}{{int64(0)}},
				},
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"first", "test", ""},
					Results: [][]interface{}{{int64(5)}},
				},
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{"second", "test", ""},
					Results: [][]interface{}{{int64(6)}},
				},
			},
			getIDCheck: func(t *testing.T, batch operationBatch) {
				id, err := batch.GetID("first", "test", "")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 5, Valid: true}, id)

				id, err = batch.GetID("", "nonexistant", "")
				require.EqualError(t, err, "error getting ID for operation { nonexistant }: error getting ID from batch")
				require.Equal(t, pgtype.Int8{Valid: false}, id)

				id, err = batch.GetID("", "zero", "")
				require.EqualError(t, err, "error getting ID for operation { zero }: ID is 0")
				require.Equal(t, pgtype.Int8{Valid: false}, id)

				id, err = batch.GetID("", "null", "")
				require.EqualError(t, err, "error getting ID for operation { null }: ID is null")
				require.Equal(t, pgtype.Int8{Valid: false}, id)
			},
		},
		{
			name:               "all urls in cache",
			operations:         []operation{{"", "incache", ""}},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch operationBatch) {
				id, err := batch.GetID("", "incache", "")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int64: 1337, Valid: true}, id)
			},
		},
		{
			name:               "send batch error",
			operations:         []operation{{defaultServiceName, "non-cached url", defaultSpanKind}},
			expectedBatchQueue: 1,
			sendBatchError:     fmt.Errorf("some error"),
			expectedError:      "some error",
		},
		{
			name:               "scan error",
			operations:         []operation{{defaultServiceName, "non-cached", defaultSpanKind}},
			expectedBatchQueue: 1,
			queries: []model.SqlQuery{
				{
					Sql:     insertOperationSQL,
					Args:    []interface{}{defaultServiceName, "non-cached", defaultSpanKind},
					Results: [][]interface{}{{"wrong type"}},
				},
			},
			expectedError: `strconv.ParseInt: parsing "wrong type": invalid syntax`,
		},
		{
			name:               "cache error",
			operations:         []operation{{"", "invalid", ""}},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch operationBatch) {
				_, err := batch.GetID("", "invalid", "")
				require.ErrorIs(t, err, errors.ErrInvalidCacheEntryType)
			},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			batch := newOperationBatch(cache)
			require.Len(t, batch.b.batch, 0)

			for _, op := range c.operations {
				batch.Queue(op.serviceName, op.spanName, op.spanKind)
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
