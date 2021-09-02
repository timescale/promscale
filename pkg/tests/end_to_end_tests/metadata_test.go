// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	metadataAPI "github.com/timescale/promscale/pkg/pgmodel/metadata"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tests/common"
)

func TestMetricMetadataIngestion(t *testing.T) {
	ts := common.GenerateSmallTimeseries()
	metadata := common.GenerateRandomMetricMetadata(20)

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		// Ingest just metadata.
		wr := ingstr.NewWriteRequest()
		wr.Metadata = copyMetadata(metadata)
		numSamples, numMetadata, err := ingestor.Ingest(wr)
		require.NoError(t, err)
		require.Equal(t, 0, int(numSamples))
		require.Equal(t, 20, int(numMetadata))

		// Ingest just time-series.
		wr = newWriteRequestWithTs(copyMetrics(ts))
		numSamples, numMetadata, err = ingestor.Ingest(wr)
		require.NoError(t, err)
		require.Equal(t, 10, int(numSamples))
		require.Equal(t, 0, int(numMetadata))

		// Ingest metadata and time-series simultaneously.
		// Note: Right now, Prometheus sends metadata as a separate request that does not contain
		// any samples. But this can change in future and hence this test is there to ensure the change
		// is well supported.
		wr = ingstr.NewWriteRequest()
		wr.Timeseries = copyMetrics(ts)
		wr.Metadata = copyMetadata(metadata)
		numSamples, numMetadata, err = ingestor.Ingest(wr)
		require.NoError(t, err)
		require.Equal(t, 10, int(numSamples))
		require.Equal(t, 20, int(numMetadata))
	})
}

func TestFetchMetricMetadataAPI(t *testing.T) {
	ts := common.GenerateSmallTimeseries()
	metadata := common.GenerateRandomMetricMetadata(20)

	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, t testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()

		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()

		// Ingest data.
		wr := ingstr.NewWriteRequest()
		wr.Timeseries = copyMetrics(ts)
		wr.Metadata = copyMetadata(metadata)

		numSamples, numMetadata, err := ingestor.Ingest(wr)
		require.NoError(t, err)
		require.Equal(t, 10, int(numSamples))
		require.Equal(t, 20, int(numMetadata))

		// Fetch metric metadata.
		// -- fetch metadata without metric_name and limit --
		db = testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_reader")
		defer db.Close()

		result, err := metadataAPI.MetricQuery(pgxconn.NewPgxConn(db), "", 0)
		require.NoError(t, err)
		expected := getExpectedMap(metadata)
		for metric, md := range result {
			fromExpected, found := expected[metric]
			require.True(t, found)
			require.Equal(t, fromExpected, md)
		}

		// -- fetch metadata with metric_name --
		result, err = metadataAPI.MetricQuery(pgxconn.NewPgxConn(db), metadata[0].MetricFamilyName, 0)
		require.NoError(t, err)
		expected = getExpectedMap(metadata[:1])
		for metric, md := range result {
			fromExpected, found := expected[metric]
			require.True(t, found)
			require.Equal(t, fromExpected, md)
		}

		// -- fetch metadata with limit --
		result, err = metadataAPI.MetricQuery(pgxconn.NewPgxConn(db), "", 5)
		require.NoError(t, err)
		require.Equal(t, 5, len(result))

		// -- fetch metadata with both limit and metric_name --
		result, err = metadataAPI.MetricQuery(pgxconn.NewPgxConn(db), metadata[0].MetricFamilyName, 1)
		require.NoError(t, err)
		require.NoError(t, err)
		require.Equal(t, 1, len(result))
		require.Equal(t, 1, len(result[metadata[0].MetricFamilyName]))
		require.Equal(t, expected[metadata[0].MetricFamilyName], result[metadata[0].MetricFamilyName])
	})
}

func getExpectedMap(m []prompb.MetricMetadata) map[string][]model.Metadata {
	result := make(map[string][]model.Metadata)
	for i := range m {
		result[m[i].MetricFamilyName] = append(result[m[i].MetricFamilyName], model.Metadata{
			Unit: m[i].Unit,
			Type: m[i].Type.String(),
			Help: m[i].Help,
		})
	}
	return result
}
