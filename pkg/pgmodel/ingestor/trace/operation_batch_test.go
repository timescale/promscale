package trace

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
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
	cache.Insert(incache, pgtype.Int8{Int: 1337, Status: pgtype.Present}, incache.SizeInCache())
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
				operation{"second", "test", ""},
				operation{"first", "test", ""},
				operation{"", "anotherTest", "second"},
				operation{"", "anotherTest", "first"},
				operation{"", "null", ""},
				operation{"", "zero", ""},
				operation{"", "incache", ""},
			},
			expectedBatchQueue: 7,
			queries: []model.SqlQuery{
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"", "anotherTest", "first"},
					Results: [][]interface{}{{int64(7)}},
				},
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"", "anotherTest", "second"},
					Results: [][]interface{}{{int64(8)}},
				},
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"", "null", ""},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"", "zero", ""},
					Results: [][]interface{}{{int64(0)}},
				},
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"first", "test", ""},
					Results: [][]interface{}{{int64(5)}},
				},
				{
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
					Args:    []interface{}{"second", "test", ""},
					Results: [][]interface{}{{int64(6)}},
				},
			},
			getIDCheck: func(t *testing.T, batch operationBatch) {
				id, err := batch.GetID("first", "test", "")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int: 5, Status: pgtype.Present}, id)

				id, err = batch.GetID("", "nonexistant", "")
				require.EqualError(t, err, "error getting ID for operation { nonexistant }: error getting ID from batch")
				require.Equal(t, pgtype.Int8{Status: pgtype.Null}, id)

				id, err = batch.GetID("", "zero", "")
				require.EqualError(t, err, "error getting ID for operation { zero }: ID is 0")
				require.Equal(t, pgtype.Int8{Status: pgtype.Null}, id)

				id, err = batch.GetID("", "null", "")
				require.EqualError(t, err, "error getting ID for operation { null }: ID is null")
				require.Equal(t, pgtype.Int8{Status: pgtype.Null}, id)
			},
		},
		{
			name:               "all urls in cache",
			operations:         []operation{{"", "incache", ""}},
			expectedBatchQueue: 1,
			getIDCheck: func(t *testing.T, batch operationBatch) {
				id, err := batch.GetID("", "incache", "")
				require.Nil(t, err)
				require.Equal(t, pgtype.Int8{Int: 1337, Status: pgtype.Present}, id)
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
					Sql:     fmt.Sprintf(insertOperationSQL, schema.TracePublic),
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
