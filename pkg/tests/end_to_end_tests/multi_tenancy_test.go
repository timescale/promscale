package end_to_end_tests

import (
	"fmt"
	"sort"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tenancy"
)

func TestMultiTenancyWithoutValidTenants(t *testing.T) {
	ts, tenants := generateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// Without valid tenants.
		cfg := tenancy.NewAllowAllTenantsConfig(false)
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, pgxconn.NewPgxConn(db), mt)
		require.NoError(t, err)
		defer client.Close()

		for _, tenant := range tenants {
			_, err = client.Ingest(ingstr.Request{Tenant: tenant, Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))})
			require.NoError(t, err)
		}

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		defer dbConn.Close()
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, mt.ReadAuthorizer())

		// ----- query a single tenant (tenant-a) -----
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

		// ----- query across multiple tenants (tenant-a & tenant-c) -----
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

		// ----- query without tenant matcher -----
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
	ts, tenants := generateSmallMultiTenantTimeseries()
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// With valid tenants.
		cfg := tenancy.NewSelectiveTenancyConfig(tenants[:2], false) // valid tenant-a & tenant-b.
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, pgxconn.NewPgxConn(db), mt)
		require.NoError(t, err)
		defer client.Close()

		// Ingest tenant-a.
		_, err = client.Ingest(ingstr.Request{Tenant: tenants[0], Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))})
		require.NoError(t, err)
		// Ingest tenant-b.
		_, err = client.Ingest(ingstr.Request{Tenant: tenants[1], Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))})
		require.NoError(t, err)
		// Ingest tenant-c.
		_, err = client.Ingest(ingstr.Request{Tenant: tenants[2], Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))}) // Invalid ingestion, so this should not get ingested.
		require.Error(t, err)

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		defer dbConn.Close()
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, mt.ReadAuthorizer())

		// ----- query a single tenant (tenant-a) -----
		var expectedResult []prompb.TimeSeries

		result, err := qr.Query(&prompb.Query{
			Matchers: []*prompb.LabelMatcher{
				{
					Type:  prompb.LabelMatcher_EQ,
					Name:  model.MetricNameLabelName,
					Value: "firstMetric",
				},
				{
					Type:  prompb.LabelMatcher_RE,
					Name:  tenancy.TenantLabelKey,
					Value: "tenant-c", // Done knowingly, but tenant-c should not be in the result, as that's an invalid tenant.
				},
			},
			StartTimestampMs: 2,
			EndTimestampMs:   4,
		})
		require.NoError(t, err)

		// Verifying result.
		verifyResults(t, expectedResult, result)

		// ----- query across multiple tenants (tenant-a & tenant-c) -----
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
	})
}

func TestMultiTenancyWithValidTenantsAndNonTenantOps(t *testing.T) {
	ts, tenants := generateSmallMultiTenantTimeseries()

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// With valid tenants and non-tenant operations are allowed.
		cfg := tenancy.NewSelectiveTenancyConfig(tenants[:2], true) // valid tenant-a & tenant-b.
		mt, err := tenancy.NewAuthorizer(cfg)
		require.NoError(t, err)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{}, 1, pgxconn.NewPgxConn(db), mt)
		require.NoError(t, err)
		defer client.Close()

		// Ingest tenant-a.
		_, err = client.Ingest(ingstr.Request{Tenant: tenants[0], Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))})
		require.NoError(t, err)

		_, err = client.Ingest(ingstr.Request{Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))}) // Ingest without tenants.
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
		_, err = client.Ingest(ingstr.Request{Req: ingstr.NewWriteRequestWithTs(copyMetrics(ts))}) // Non-MT write.
		require.NoError(t, err)

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
		defer dbConn.Close()
		labelsReader := lreader.NewLabelsReader(dbConn, lCache)
		qr := querier.NewQuerier(dbConn, mCache, labelsReader, mt.ReadAuthorizer())

		// ----- query a single tenant (tenant-a) -----
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
		verifyResults(t, expectedResult, result) // Fails: expected to be 1, but shows 0 result.

		// ----- query across multiple tenants (tenant-a & tenant-c) -----
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
