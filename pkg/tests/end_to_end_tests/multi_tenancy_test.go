package end_to_end_tests

import (
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/clockcache"
	multi_tenancy "github.com/timescale/promscale/pkg/multi-tenancy"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/lreader"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgmodel/querier"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"sort"
	"testing"

	"github.com/timescale/promscale/pkg/multi-tenancy/config"
)

func TestMultiTenancyPlainWithoutValidTenants(t *testing.T) {
	ts, tenants := generateSmallMultiTenantTimeseries()
	withDB(t, "multi_tenancy_plain_db_test", func(db *pgxpool.Pool, t testing.TB) {
		// Without valid tenants.
		cfg := &config.Config{
			AuthType: config.Allow,
		}
		err := cfg.Validate()
		require.NoError(t, err)
		mt, err := multi_tenancy.NewMultiTenancy(cfg)
		require.NoError(t, err)

		c := &cache.MetricNameCache{Metrics: clockcache.WithMax(cache.DefaultMetricCacheSize)}
		s := cache.NewSeriesCache(cache.DefaultSeriesCacheSize)

		// Ingestion.
		ingestor, err := ingstr.NewPgxIngestorWithMetricCache(pgxconn.NewPgxConn(db), c, s, &ingstr.Cfg{}, mt.WriteAuthorizer())
		require.NoError(t, err)
		defer ingestor.Close()
		for _, tenant := range tenants {
			_, err = ingestor.Ingest(tenant, copyMetrics(ts), ingstr.NewWriteRequest())
			require.NoError(t, err)
		}

		// Ingest tenant-a.

		_, err = ingestor.Ingest(tenants[0], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-b.
		_, err = ingestor.Ingest(tenants[1], ts, ingstr.NewWriteRequest())
		require.NoError(t, err)
		// Ingest tenant-c.
		_, err = ingestor.Ingest(tenants[2], ts, ingstr.NewWriteRequest())
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
		for k := 0; k < len(expectedResult); k++ {
			sort.SliceStable(result[k].Labels, func(i, j int) bool {
				return result[k].Labels[i].Name < result[k].Labels[j].Name
			})
			sort.SliceStable(expectedResult[k].Labels, func(i, j int) bool {
				return expectedResult[k].Labels[i].Name < expectedResult[k].Labels[j].Name
			})
			require.Equal(t, expectedResult[k].Labels, result[k].Labels)
			require.Equal(t, expectedResult[k].Samples, result[k].Samples)
		}

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
		for k := 0; k < len(expectedResult); k++ {
			sort.SliceStable(result[k].Labels, func(i, j int) bool {
				return result[k].Labels[i].Name < result[k].Labels[j].Name
			})
			sort.SliceStable(expectedResult[k].Labels, func(i, j int) bool {
				return expectedResult[k].Labels[i].Name < expectedResult[k].Labels[j].Name
			})
			require.Equal(t, expectedResult[k].Labels, result[k].Labels)
			require.Equal(t, expectedResult[k].Samples, result[k].Samples)
		}
	})
}
