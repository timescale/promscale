// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tenancy"
	"github.com/timescale/promscale/pkg/tests/common"
)

func TestMultiTenancyWithoutValidTenants(t *testing.T) {
	ts, tenants := common.GenerateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Without valid tenants.
		cfg := tenancy.NewAllowAllTenantsConfig(false)
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, db, mt, false)
		require.NoError(t, err)
		defer client.Close()

		for _, tenant := range tenants {
			request := newWriteRequestWithTs(copyMetrics(ts))
			// Pre-processing.
			wauth := mt.WriteAuthorizer()
			err = wauth.Process(requestWithHeaderTenant(tenant), request)
			require.NoError(t, err)
			_, _, err = client.Ingest(request)
			require.NoError(t, err)
		}

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		// ----- query-test: querying a single tenant (tenant-a) -----
		expectedResult := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "firstMetric"},
					{Name: "foo", Value: "bar"},
					{Name: "common", Value: "tag"},
					{Name: "empty", Value: ""},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 0.2},
					{Timestamp: 3, Value: 0.3},
					{Timestamp: 4, Value: 0.4},
				},
			},
		}

		result, err := qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "firstMetric",
				},
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-a",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-test: querying across multiple tenants (tenant-a & tenant-c) -----
		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-c"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-a|tenant-c",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-test: querying without tenant matcher -----
		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-c"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)
	})
}

func TestMultiTenancyWithValidTenants(t *testing.T) {
	ts, tenants := common.GenerateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// With valid tenants.
		cfg := tenancy.NewSelectiveTenancyConfig(tenants[:2], false) // valid tenant-a & tenant-b.
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, db, mt, false)
		require.NoError(t, err)
		defer client.Close()

		wauth := mt.WriteAuthorizer()
		// Ingest tenant-a.
		request := newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(requestWithHeaderTenant(tenants[0]), request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)

		// Ingest tenant-b.
		request = newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(requestWithHeaderTenant(tenants[1]), request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)
		require.NoError(t, err)

		// Ingest tenant-c.
		request = newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(requestWithHeaderTenant(tenants[2]), request)
		require.Error(t, err)
		require.Equal(t, err.Error(), "write-authorizer process: authorization error for tenant tenant-c: unauthorized or invalid tenant")

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		// ----- query-test: querying a valid tenant (tenant-a) -----
		expectedResult := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}
		result, err := qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-a",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-test: querying an invalid tenant (tenant-c) -----
		expectedResult = []prompb.TimeSeries{}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "firstMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-c",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-test: querying across multiple tenants (tenant-a & tenant-b) -----
		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-a|tenant-b",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// query-test: ingested by one org, and being queried by some other org, so no result should happen.
		//
		// eg: tenant-a and tenant-b is ingested. Now, a reader who is just authorized to read tenant-a,
		// tries tenant-b should get empty result.
		cfg = tenancy.NewSelectiveTenancyConfig(tenants[:1], false) // valid tenant-a only.
		mt, err = tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		labelsReader = lreader.NewLabelsReader(dbConn, lCache)
		qr = querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		expectedResult = []prompb.TimeSeries{}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-b",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)
	})
}

func TestMultiTenancyWithValidTenantsAndNonTenantOps(t *testing.T) {
	ts, tenants := common.GenerateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// With valid tenants and non-tenant operations are allowed.
		cfg := tenancy.NewSelectiveTenancyConfig(tenants[:2], true) // valid tenant-a & tenant-b.
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, db, mt, false)
		require.NoError(t, err)
		defer client.Close()

		wauth := mt.WriteAuthorizer()
		// Ingest tenant-a.
		request := newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(requestWithHeaderTenant(tenants[0]), request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)

		// Ingest tenant-b.
		request = newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(requestWithHeaderTenant(tenants[1]), request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)

		ts = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "thirdMetric"},
					{Name: "foo", Value: "bar"},
					{Name: "common", Value: "tag"},
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
		// Ingest without tenants.
		request = newWriteRequestWithTs(copyMetrics(ts))
		err = wauth.Process(&http.Request{}, request) // Ingest without tenants.
		require.NoError(t, err)
		_, _, err = client.Ingest(request) // Non-MT write.
		require.NoError(t, err)

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		// ----- query-test: querying a non-tenant -----
		expectedResult := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "thirdMetric"},
					{Name: "foo", Value: "bar"},
					{Name: "common", Value: "tag"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 0.2},
					{Timestamp: 3, Value: 0.3},
					{Timestamp: 4, Value: 0.4},
				},
			},
		}

		result, err := qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "thirdMetric",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-tests: querying across multiple tenants (tenant-a & tenant-b) -----
		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  "job",
					Value: "baz",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// query-test: ingested by one org with NonMT true, and being queried by some other org with NonMT false,
		// so result should contain MT writes of valid tenants by the later org.
		cfg = tenancy.NewSelectiveTenancyConfig(tenants[:2], false) // valid tenant-a & tenant-b.
		mt, err = tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		labelsReader = lreader.NewLabelsReader(dbConn, lCache)
		qr = querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		expectedResult = []prompb.TimeSeries{}
		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "thirdMetric",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)
	})
}

func TestMultiTenancyWithValidTenantsAsLabels(t *testing.T) {
	ts, tenants := common.GenerateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// With valid tenants.
		cfg := tenancy.NewSelectiveTenancyConfig(tenants[:2], false) // valid tenant-a & tenant-b.
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, db, mt, false)
		require.NoError(t, err)
		defer client.Close()

		wauth := mt.WriteAuthorizer()
		// Ingest tenant-a.
		request := newWriteRequestWithTs(applyTenantInLabels(tenants[0], copyMetrics(ts)))
		err = wauth.Process(&http.Request{}, request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)

		// Ingest tenant-b.
		request = newWriteRequestWithTs(applyTenantInLabels(tenants[1], copyMetrics(ts)))
		err = wauth.Process(&http.Request{}, request)
		require.NoError(t, err)
		_, _, err = client.Ingest(request)
		require.NoError(t, err)
		require.NoError(t, err)

		// Ingest tenant-c.
		request = newWriteRequestWithTs(applyTenantInLabels(tenants[2], copyMetrics(ts)))
		err = wauth.Process(&http.Request{}, request)
		require.Error(t, err)
		require.Equal(t, err.Error(), "write-authorizer process: authorization error for tenant tenant-c: unauthorized or invalid tenant")

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, nil, mt.ReadAuthorizer())

		// ----- query-test: querying a single tenant (tenant-b) -----
		expectedResult := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err := qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-b",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query-test: querying across multiple tenants (tenant-a & tenant-b) -----
		expectedResult = []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-a"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabelName, Value: "secondMetric"},
					{Name: "job", Value: "baz"},
					{Name: "ins", Value: "tag"},
					{Name: tenancy.TenantLabelKey, Value: "tenant-b"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 2, Value: 2.2},
					{Timestamp: 3, Value: 2.3},
					{Timestamp: 4, Value: 2.4},
				},
			},
		}

		result, err = qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "secondMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-a|tenant-b",
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)
	})
}

func verifyResults(t testing.TB, expectedResult []prompb.TimeSeries, receivedResult []*prompb.TimeSeries) {
	if len(receivedResult) != len(expectedResult) {
		require.Fail(t, fmt.Sprintf("lengths of result (%d) and expectedResult (%d) does not match", len(receivedResult), len(expectedResult)))
	}
	for k := 0; k < len(receivedResult); k++ {
		sort.SliceStable(receivedResult[k].Labels, func(i, j int) bool {
			return receivedResult[k].Labels[i].Name < receivedResult[k].Labels[j].Name
		})
		sort.SliceStable(expectedResult[k].Labels, func(i, j int) bool {
			return expectedResult[k].Labels[i].Name < expectedResult[k].Labels[j].Name
		})
		require.Equal(t, expectedResult[k].Labels, receivedResult[k].Labels)
		require.Equal(t, expectedResult[k].Samples, receivedResult[k].Samples)
	}
}

func requestWithHeaderTenant(tenant string) *http.Request {
	header := make(http.Header)
	header.Add("TENANT", tenant)
	return &http.Request{Header: header}
}

func applyTenantInLabels(tenant string, ts []prompb.TimeSeries) []prompb.TimeSeries {
	for i := 0; i < len(ts); i++ {
		ts[i].Labels = append(ts[i].Labels, prompb.Label{Name: tenancy.TenantLabelKey, Value: tenant})
	}
	return ts
}
