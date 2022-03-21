// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

func TestInsertInCompressedChunks(t *testing.T) {
	ts := generateSmallTimeseries()
	if !*useTimescaleDB {
		// Ingest in plain postgres to ensure everything works well even if TimescaleDB is not installed.
		withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
			ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
			require.NoError(t, err)
			defer ingestor.Close()
			_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
			require.NoError(t, err)
			r, err := db.Query(context.Background(), "SELECT * from prom_data.\"firstMetric\";")
			require.NoError(t, err)
			defer r.Close()

			count := 0
			for r.Next() {
				count++
			}
			require.Equal(t, 5, count)
		})
		return
	}

	sample := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabelName, Value: "firstMetric"},
				{Name: "foo", Value: "bar"},
				{Name: "common", Value: "tag"},
				{Name: "empty", Value: ""},
			},
			Samples: []prompb.Sample{
				{Timestamp: 7, Value: 0.7},
			},
		},
	}
	// With decompress chunks being true.
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		require.NoError(t, err)
		defer ingestor.Close()
		_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.compress_chunk(i) from public.show_chunks('prom_data.\"firstMetric\"') i;")
		require.NoError(t, err)

		// Insert data into compressed chunk.
		_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(sample)))
		require.NoError(t, err)

		r, err := db.Query(context.Background(), "SELECT * from prom_data.\"firstMetric\";")
		require.NoError(t, err)
		defer r.Close()

		count := 0
		for r.Next() {
			count++
		}
		require.Equal(t, 6, count)
	})

	// With decompress chunks being false.
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), &ingstr.Cfg{IgnoreCompressedChunks: true})
		require.NoError(t, err)
		defer ingestor.Close()
		_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts)))
		require.NoError(t, err)
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(context.Background(), "SELECT prom_api.compress_chunk(i) from public.show_chunks('prom_data.\"firstMetric\"') i;")
		require.NoError(t, err)

		// Insert data into compressed chunk.
		_, _, err = ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(sample)))
		require.NoError(t, err)

		r, err := db.Query(context.Background(), "SELECT * from prom_data.\"firstMetric\";")
		require.NoError(t, err)
		defer r.Close()

		count := 0
		for r.Next() {
			count++
		}
		require.Equal(t, 5, count) // The recent sample did not get ingested. This is because the chunks were compressed and we were asked to not ingest into compressed chunks.
	})
}
