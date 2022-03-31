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

func TestHasPermissionOnTable(t *testing.T) {
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
		_, err = db.Exec(context.Background(), "create table public.foo (bar int)")
		if err != nil {
			t.Fatal(err)
		}
		tests := [][]interface{}{
			{"_ps_trace.span", 1},
			{`prom_data."firstMetric"`, 1},
			{`prom_data."secondMetric"`, 1},
			{"public.foo", 0},                // not a part of the extension
			{"ps_tag.tag_op_not_equals", -1}, // not a table
		}
		qry := `select case _prom_catalog.has_permission_on_table($1::regclass) when true then 1 when false then 0 else -1 end`
		var actual int
		for _, expected := range tests {
			err = db.QueryRow(context.Background(), qry, expected[0]).Scan(&actual)
			if err != nil {
				t.Fatal(err)
			}
			if expected[1] != actual {
				t.Errorf("_prom_catalog.has_permission_on_table did not produce the expected results. expected: %d actual %d", expected[1], actual)
			}
		}
	})
}
