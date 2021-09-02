// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgutf8str"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tests/common"
)

var (
	testNullLabelVal            = fmt.Sprintf("fo%co", pgutf8str.NullChar)
	testNullLabelValReplacement = fmt.Sprintf("fo%co", pgutf8str.NullCharSanitize)
	testNullTagValue            = fmt.Sprintf("ta%cg", pgutf8str.NullChar)
	testNullTagValReplacement   = fmt.Sprintf("ta%cg", pgutf8str.NullCharSanitize)
)

func TestOperationWithNullChars(t *testing.T) {
	ts := common.GenerateSmallTimeseries()
	// Apply null chars in ts.
	for i := range ts {
		ts[i].Labels = applyNullCharsToFoo(t, ts[i].Labels) // Insert null char in label key.
		ts[i].Labels = applyNullCharsToTag(t, ts[i].Labels) // insert null char in label value.
	}
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		_, _, err = ingestor.Ingest(newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)

		// Verify sanitization is ingested in the db.
		var key string
		err = db.QueryRow(context.Background(), fmt.Sprintf("select key from _prom_catalog.label where key='%s'", testNullLabelValReplacement)).Scan(&key)
		require.NoError(t, err)
		if key != testNullLabelValReplacement {
			require.Fail(t, "sanitization does not exist in the db")
		}
		err = db.QueryRow(context.Background(), "select value from _prom_catalog.label where key='common'").Scan(&key)
		require.NoError(t, err)
		if key != testNullTagValReplacement {
			require.Fail(t, "sanitization does not exist in the db")
		}

		// Verify if revert of sanitized null char happens while querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		r := querier.NewQuerier(dbConn, mCache, labelsReader, nil, nil)
		resp, err := r.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "firstMetric",
				},
			},
			StartTimestampMs: 1,
			EndTimestampMs:   5,
		})
		require.NoError(t, err)
		for _, l := range resp[0].Labels {
			if l.Name == testNullLabelValReplacement {
				require.Fail(t, "reverting of null sanitized string did not happen")
			}
			if l.Value == testNullLabelValReplacement {
				require.Fail(t, "reverting of null sanitized string did not happen")
			}
		}

		// Verify response.
		expectedResponse := []*prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "firstMetric"},
					{Name: "common", Value: "ta\x00g"},
					{Name: "empty", Value: ""},
					{Name: "fo\x00o", Value: "bar"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
					{Timestamp: 3, Value: 0.3},
					{Timestamp: 4, Value: 0.4},
					{Timestamp: 5, Value: 0.5},
				},
			},
		}
		if !reflect.DeepEqual(resp, expectedResponse) {
			require.Fail(t, "response not equal", "expected", expectedResponse, "received", resp[0])
		}
	})
}

func applyNullCharsToFoo(t *testing.T, l []prompb.Label) []prompb.Label {
	var (
		hasFoo  bool
		newLSet = make([]prompb.Label, len(l))
	)
	for i, lbl := range l {
		if lbl.Name == "foo" {
			// Ensure that in future, if the common.GenerateSmallTimeseries() is changed, then this test fails as well.
			hasFoo = true
			lbl.Name = testNullLabelVal
		}
		newLSet[i].Name = lbl.Name
		newLSet[i].Value = lbl.Value
	}
	if !hasFoo {
		require.Fail(t, "foo label needs to be present in order to insert null chars")
	}
	return newLSet
}

func applyNullCharsToTag(t *testing.T, l []prompb.Label) []prompb.Label {
	var (
		hasTag  bool
		newLSet = make([]prompb.Label, len(l))
	)
	for i, lbl := range l {
		if lbl.Value == "tag" {
			// Ensure that in future, if the common.GenerateSmallTimeseries() is changed, then this test fails as well.
			hasTag = true
			lbl.Value = testNullTagValue
		}
		newLSet[i].Name = lbl.Name
		newLSet[i].Value = lbl.Value
	}
	if !hasTag {
		require.Fail(t, "foo label needs to be present in order to insert null chars")
	}
	return newLSet
}
