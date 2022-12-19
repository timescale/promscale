// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package trace

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/pgmodel/model"
)

func TestTagBatch(t *testing.T) {
	cache := newTagCache()
	incache := tag{"incache", `""`, SpanTagType}
	invalid := tag{"invalid", `""`, SpanTagType}
	cache.Insert(incache, tagIDs{pgtype.Int8{Int64: 1, Valid: true}, pgtype.Int8{Int64: 2, Valid: true}}, incache.SizeInCache())
	cache.Insert(invalid, "foo", 0)

	testCases := []struct {
		name               string
		tags               map[TagType]map[string]interface{}
		queueError         string
		expectedBatchQueue int
		queries            []model.SqlQuery
		sendBatchError     error
		expectedError      string
		getTagMapJSONCheck func(t *testing.T, batch tagBatch)
	}{
		{
			name: "happy path",
			tags: map[TagType]map[string]interface{}{
				SpanTagType: {
					"test":    "",
					"second":  "anotherTest",
					"first":   "anotherTest",
					"null":    "",
					"zero":    "",
					"incache": "",
				},
				EventTagType: {
					"test": "",
				},
				ResourceTagType: {
					"test": "first",
				},
			},
			expectedBatchQueue: 8,
			queries: []model.SqlQuery{
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"first", SpanTagType},
					Results: [][]interface{}{{int64(1)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"first", `"anotherTest"`, SpanTagType},
					Results: [][]interface{}{{int64(2)}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"null", SpanTagType},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"null", `""`, SpanTagType},
					Results: [][]interface{}{{nil}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"second", SpanTagType},
					Results: [][]interface{}{{int64(3)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"second", `"anotherTest"`, SpanTagType},
					Results: [][]interface{}{{int64(4)}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"test", SpanTagType},
					Results: [][]interface{}{{int64(7)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"test", `""`, SpanTagType},
					Results: [][]interface{}{{int64(8)}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"test", EventTagType},
					Results: [][]interface{}{{int64(9)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"test", `""`, EventTagType},
					Results: [][]interface{}{{int64(10)}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"test", ResourceTagType},
					Results: [][]interface{}{{int64(5)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"test", `"first"`, ResourceTagType},
					Results: [][]interface{}{{int64(6)}},
				},
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"zero", SpanTagType},
					Results: [][]interface{}{{int64(0)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"zero", `""`, SpanTagType},
					Results: [][]interface{}{{int64(0)}},
				},
			},
			getTagMapJSONCheck: func(t *testing.T, batch tagBatch) {
				tagMap, err := batch.GetTagMapJSON(map[string]interface{}{"test": ""}, SpanTagType)
				require.Nil(t, err)
				require.Equal(t, `{"7":8}`, string(tagMap))

				tagMap, err = batch.GetTagMapJSON(map[string]interface{}{"nonexistant": ""}, SpanTagType)
				require.EqualError(t, err, "error getting tag from batch {nonexistant \"\" 1}: error getting item from batch")
				require.Equal(t, []byte(nil), tagMap)

				tagMap, err = batch.GetTagMapJSON(map[string]interface{}{"zero": ""}, SpanTagType)
				require.EqualError(t, err, "tag IDs have 0 values: trace.tagIDs{keyID:pgtype.Int8{Int64:0, Valid:true}, valueID:pgtype.Int8{Int64:0, Valid:true}}")
				require.Equal(t, []byte(nil), tagMap)

				tagMap, err = batch.GetTagMapJSON(map[string]interface{}{"null": ""}, SpanTagType)
				require.EqualError(t, err, "tag IDs have NULL values: trace.tagIDs{keyID:pgtype.Int8{Int64:0, Valid:false}, valueID:pgtype.Int8{Int64:0, Valid:false}}")
				require.Equal(t, []byte(nil), tagMap)

				tagMap, err = batch.GetTagMapJSON(map[string]interface{}{"test": make(chan struct{})}, SpanTagType)
				require.EqualError(t, err, "json: unsupported type: chan struct {}")
				require.Equal(t, []byte(nil), tagMap)
			},
		},
		{
			name:               "all urls in cache",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"incache": ""}},
			expectedBatchQueue: 1,
			getTagMapJSONCheck: func(t *testing.T, batch tagBatch) {
				tagMap, err := batch.GetTagMapJSON(map[string]interface{}{"incache": ""}, SpanTagType)
				require.Nil(t, err)
				require.Equal(t, `{"1":2}`, string(tagMap))
			},
		},
		{
			name:               "send batch error",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"key": "value"}},
			expectedBatchQueue: 1,
			sendBatchError:     fmt.Errorf("some error"),
			expectedError:      "some error",
		},
		{
			name:               "scan error keyID",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"non-cached": ""}},
			expectedBatchQueue: 1,
			queries: []model.SqlQuery{
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"non-cached", SpanTagType},
					Results: [][]interface{}{{"wrong type"}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"non-cached", `""`, SpanTagType},
					Results: [][]interface{}{{int64(999)}},
				},
			},
			expectedError: `error scanning key ID: strconv.ParseInt: parsing "wrong type": invalid syntax`,
		},
		{
			name:               "scan error valueID",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"non-cached": ""}},
			expectedBatchQueue: 1,
			queries: []model.SqlQuery{
				{
					Sql:     insertTagKeySQL,
					Args:    []interface{}{"non-cached", SpanTagType},
					Results: [][]interface{}{{int64(999)}},
				},
				{
					Sql:     insertTagSQL,
					Args:    []interface{}{"non-cached", `""`, SpanTagType},
					Results: [][]interface{}{{"wrong type"}},
				},
			},
			expectedError: `error scanning value ID: strconv.ParseInt: parsing "wrong type": invalid syntax`,
		},
		{
			name:               "cache error",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"invalid": ""}},
			expectedBatchQueue: 1,
			getTagMapJSONCheck: func(t *testing.T, batch tagBatch) {
				_, err := batch.GetTagMapJSON(map[string]interface{}{"invalid": ""}, SpanTagType)
				require.EqualError(t, err, "error getting tag {invalid \"\" 1} from batch: invalid cache entry type stored")
			},
		},
		{
			name:               "queue error",
			tags:               map[TagType]map[string]interface{}{SpanTagType: {"invalid": make(chan struct{})}},
			expectedBatchQueue: 1,
			queueError:         `json: unsupported type: chan struct {}`,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			batch := newTagBatch(cache)
			require.Len(t, batch.b.batch, 0)

			for typ, tagMap := range c.tags {
				err := batch.Queue(tagMap, typ)
				if c.queueError != "" {
					require.EqualError(t, err, c.queueError)
					return
				}

				require.NoError(t, err)
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

			if c.getTagMapJSONCheck != nil {
				c.getTagMapJSONCheck(t, batch)
			}
		})
	}
}
