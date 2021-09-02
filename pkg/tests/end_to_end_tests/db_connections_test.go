// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tests/common"
)

var readRequest *prompb.ReadRequest

func TestDBConnectionHandling(t *testing.T) {
	withDB(t, *testDatabase, func(dbOwner *pgxpool.Pool, tb testing.TB) {
		db := testhelpers.PgxPoolWithRole(t, *testDatabase, "prom_writer")
		defer db.Close()
		_, pgClient, err := buildRouterWithAPIConfig(db, defaultAPIConfig())
		if err != nil {
			t.Fatalf("unexpected error while creating pgClient: %s", err)
		}
		defer pgClient.Close()

		metrics := common.GenerateLargeTimeseries()
		readRequest = &prompb.ReadRequest{
			Queries: []*prompb.Query{
				{
					StartTimestampMs: 1,
					EndTimestampMs:   2,
					Matchers: []*prompb.LabelMatcher{
						{
							Type:  prompb.LabelMatcher_EQ,
							Name:  model.MetricNameLabelName,
							Value: "metric_1",
						},
					},
				},
			},
		}

		// Ingesting first part of metrics.
		count := 0
		for _, m := range metrics[:3] {
			count += len(m.Samples)
		}
		ingested, _, err := pgClient.Ingest(newWriteRequestWithTs(metrics[:3]))
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		if int(ingested) != count {
			t.Fatalf("unexpected count of ingested metrics: got %d, wanted %d", ingested, count)
		}

		// Check for Read response.
		resp, err := pgClient.Read(readRequest)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		if resp == nil {
			t.Fatalf("got an unexpected nil response from pgClient.Read")
		}

		var user string
		err = db.QueryRow(context.Background(), "SELECT current_user").Scan(&user)
		if err != nil {
			t.Fatalf("got an unexpected error while getting DB user: %v", err)
		}

		// Block all connections to DB.
		err = blockAllConnections(db, *testDatabase, user)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}

		// Try ingesting and reading from DB, expect to error.
		_, _, err = pgClient.Ingest(newWriteRequestWithTs(metrics[3:]))
		if ignoreBlockedConnectionError(err) != nil {
			t.Fatalf("got an unexpected error: %v", err)
		}
		_, err = pgClient.Read(readRequest)
		if ignoreBlockedConnectionError(err) != nil {
			t.Fatalf("expected an error to occur: %+v", resp)
		}

		// Allow DB connections again.
		err = allowAllConnections(*testDatabase, user)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}

		// Ingesting second part of metrics.
		count = 0
		for _, m := range metrics[3:] {
			count += len(m.Samples)
		}
		ingested, _, err = pgClient.Ingest(newWriteRequestWithTs(metrics[3:]))
		if err != nil {
			t.Fatalf("got an unexpected error: %v", err)
		}
		if int(ingested) != count {
			t.Fatalf("unexpected count of ingested metrics: got %d, wanted %d", ingested, count)
		}

		// Check for Read response.
		resp, err = pgClient.Read(readRequest)
		if err != nil {
			t.Fatalf("got an unexpected error %v", err)
		}
		if resp == nil {
			t.Fatalf("got an unexpected nil response from pgClient.Read")
		}
	})
}

func ignoreBlockedConnectionError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "FATAL: permission denied for database \"tmp_db_timescale_migrate_test\" (SQLSTATE 42501)") {
		return nil
	}
	if strings.Contains(err.Error(), "FATAL: terminating connection due to administrator command (SQLSTATE 57P01)") {
		return nil
	}
	return err
}

func blockAllConnections(db *pgxpool.Pool, dbName string, user string) error {
	// Connect using superuser since our previous user has revoked connect privilege.
	dbPool, err := pgxpool.Connect(context.Background(), testhelpers.PgConnectURL(dbName, true))
	if err != nil {
		return err
	}

	defer dbPool.Close()
	if _, err := dbPool.Exec(context.Background(), "REVOKE ALL PRIVILEGES ON DATABASE "+dbName+" FROM PUBLIC, "+user); err != nil {
		return err
	}

	_, err = dbPool.Exec(context.Background(), "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity")
	return ignoreBlockedConnectionError(err)
}

func allowAllConnections(dbName string, user string) error {
	// Connect using superuser since our previous user has revoked connect privilege.
	dbPool, err := pgxpool.Connect(context.Background(), testhelpers.PgConnectURL(dbName, true))
	if err != nil {
		return err
	}

	defer dbPool.Close()
	_, err = dbPool.Exec(context.Background(), "GRANT ALL PRIVILEGES ON DATABASE "+dbName+" TO PUBLIC, "+user)
	return err
}
