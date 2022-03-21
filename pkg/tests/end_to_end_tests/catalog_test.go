// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func TestPromscaleHypertablesView(t *testing.T) {
	if !*useTimescaleDB {
		t.Skip("testing _prom_catalog.promscale_hypertables needs TimescaleDB support")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, tb testing.TB) {
		ts := generateSmallTimeseries()
		ingestor, err := ingstr.NewPgxIngestorForTests(pgxconn.NewPgxConn(db), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer ingestor.Close()
		if _, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(copyMetrics(ts))); err != nil {
			t.Fatal(err)
		}
		err = ingestor.CompleteMetricCreation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		qry := "select array_agg(row(hypertable_schema, hypertable_name) order by hypertable_schema, hypertable_name)::text from _prom_catalog.promscale_hypertables"
		expected := `{"(_ps_trace,event)","(_ps_trace,link)","(_ps_trace,span)","(prom_data,firstMetric)","(prom_data,secondMetric)"}`
		var actual string
		err = db.QueryRow(context.Background(), qry).Scan(&actual)
		if err != nil {
			t.Fatal(err)
		}
		if expected != actual {
			t.Errorf("_prom_catalog.promscale_hypertables did not produce the expected results. expected: %s actual %s", expected, actual)
		}
	})
}
