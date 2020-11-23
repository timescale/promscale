package end_to_end_tests

import (
	"context"
	"flag"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/prometheus/prompb"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	. "github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/version"
)

var (
	useExtension   = flag.Bool("use-extension", true, "use the promscale extension")
	useTimescaleDB = flag.Bool("use-timescaledb", true, "use TimescaleDB")
)

func withDB(t testing.TB, DBName string, f func(db *pgxpool.Pool, t testing.TB)) {
	testhelpers.WithDB(t, DBName, testhelpers.NoSuperuser, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
		performMigrate(t, connectURL, testhelpers.PgConnectURL(DBName, testhelpers.Superuser))

		// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
		db, err := pgxpool.Connect(context.Background(), connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		f(db, t)
	})
}

func performMigrate(t testing.TB, connectURL string, superConnectURL string) {
	if *useTimescaleDB {
		migrateURL := connectURL
		if !*useExtension {
			//The docker image without an extension does not have pgextwlist
			//Thus, you have to use the superuser to install TimescaleDB
			migrateURL = superConnectURL
		}
		err := MigrateTimescaleDBExtension(migrateURL)
		if err != nil {
			t.Fatal(err)
		}
	}
	migratePool, err := pgxpool.Connect(context.Background(), connectURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migratePool.Close()
	conn, err := migratePool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	err = Migrate(conn.Conn(), VersionInfo{Version: version.Version, CommitHash: "azxtestcommit"})
	if err != nil {
		t.Fatal(err)
	}
}

// deep copy the metrics since we mutate them, and don't want to invalidate the tests
func copyMetrics(metrics []prompb.TimeSeries) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, len(metrics))
	copy(out, metrics)
	for i := range out {
		samples := make([]prompb.Sample, len(out[i].Samples))
		copy(samples, out[i].Samples)
		out[i].Samples = samples
	}
	return out
}

//func prombToTSpromb(m []prompb.TimeSeries) []tspromb.TimeSeries {
//	ts := make([]tspromb.TimeSeries, len(m))
//	for i := range m {
//		// Convert samples.
//		tsSamples := make([]tspromb.Sample, len(m[i].Samples))
//		for s := range m[i].Samples {
//			tsSamples[s].Value = m[i].Samples[s].Value
//			tsSamples[s].Timestamp = m[i].Samples[s].Timestamp
//		}
//		ts[i].Samples = tsSamples
//		// Convert labels.
//		tsLabels := make([]tspromb.Label, len(m[i].Labels))
//		for l := range m[i].Labels {
//			tsLabels[l].Name = m[i].Labels[l].Name
//			tsLabels[l].Value = m[i].Labels[l].Value
//		}
//		ts[i].Labels = tsLabels
//	}
//	return ts
//}
