package end_to_end_tests

import (
	"github.com/timescale/promscale/pkg/pgclient"
	"sort"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	multi_tenancy "github.com/timescale/promscale/pkg/multi-tenancy"
	"github.com/timescale/promscale/pkg/multi-tenancy/config"
	multi_tenancy_config "github.com/timescale/promscale/pkg/multi-tenancy/config"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestMultiTenancyWithoutValidTenants(t *testing.T) {
	ts, tenants := generateSmallMultiTenantTimeseries()
	withDB(t, "multi_tenancy_plain_db_test", func(db *pgxpool.Pool, t testing.TB) {
		// Without valid tenants.
		cfg := multi_tenancy_config.NewOpenTenancyConfig()
		mt, err := multi_tenancy.NewMultiTenancy(cfg)
		require.NoError(t, err)

		//c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		//s := cache.NewSeriesCache(cache.DefaultConfig, nil)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{HAEnabled: false}, 1, pgxconn.NewPgxConn(db), mt)
		require.NoError(t, err)

		authr := mt.WriteAuthorizer()
		for _, tenant := range tenants {
			require.True(t, authr.IsAuthorized(tenant)) // Note: Token checks are done on the API module and is not a business of multi-tenancy module.
			_, err = client.Ingest(tenant, copyMetrics(ts), ingstr.NewWriteRequest())
			require.NoError(t, err)
		}

		// Ingest tenant-a.
		_, err = client.Ingest(tenants[0], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-b.
		_, err = client.Ingest(tenants[1], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-c.
		_, err = client.Ingest(tenants[2], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
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
					{Name: config.TenantLabelKey, Value: "tenant-a"},
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
					Name:  config.TenantLabelKey,
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
					{Name: config.TenantLabelKey, Value: "tenant-a"},
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
					{Name: config.TenantLabelKey, Value: "tenant-c"},
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
					Name:  config.TenantLabelKey,
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

func TestMultiTenancyWithValidTenants(t *testing.T) {
	ts, tenants := generateSmallMultiTenantTimeseries()
	withDB(t, "multi_tenancy_plain_db_test", func(db *pgxpool.Pool, t testing.TB) {
		// Without valid tenants.
		cfg := multi_tenancy_config.NewSelectiveTenancyConfig(tenants[:2]) // valid tenant-a & tenant-b.
		mt, err := multi_tenancy.NewMultiTenancy(cfg)
		require.NoError(t, err)

		//c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		//s := cache.NewSeriesCache(cache.DefaultConfig, nil)

		// Ingestion.
		client, err := pgclient.NewClientWithPool(&pgclient.Config{HAEnabled: false}, 1, pgxconn.NewPgxConn(db), mt)
		require.NoError(t, err)

		authr := mt.WriteAuthorizer()
		for _, tenant := range tenants {
			if tenant != tenants[2] { // As tenant-c is unauthorized.
				require.True(t, authr.IsAuthorized(tenant))
			} else {
				require.False(t, authr.IsAuthorized(tenant))
			}
			_, err = client.Ingest(tenant, copyMetrics(ts), ingstr.NewWriteRequest())
			require.NoError(t, err)
		}
		// Ingest again, but with invalid tokens. But, this should not matter as plain type does not checks for tokens.
		for _, tenant := range tenants {
			if tenant != tenants[2] { // As tenant-c is unauthorized.
				require.True(t, authr.IsAuthorized(tenant)) // Plain type of multi-tenancy, hence this should be true always expect on tenant-c.
			} else {
				// tenant-c is unauthrized so false should be reported by the IsAuthorized().
				require.False(t, authr.IsAuthorized(tenant))
			}
		}

		// Ingest tenant-a.
		_, err = client.Ingest(tenants[0], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-b.
		_, err = client.Ingest(tenants[1], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-c.
		_, err = client.Ingest(tenants[2], ts, ingstr.NewWriteRequest()) // Invalid ingestion, so this should not get ingested.
		require.NoError(t, err)

		// Querying.
		mCache := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		lCache := clockcache.WithMax(100)
		dbConn := pgxconn.NewPgxConn(db)
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
					Name:  config.TenantLabelKey,
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
					{Name: config.TenantLabelKey, Value: "tenant-a"},
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
					Name:  config.TenantLabelKey,
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

func verifyResults(t testing.TB, expectedResult []prompb.TimeSeries, receivedResult []*prompb.TimeSeries) {
	if len(receivedResult) != len(expectedResult) {
		require.Fail(t, "lengths of result and expectedResult does not match")
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
