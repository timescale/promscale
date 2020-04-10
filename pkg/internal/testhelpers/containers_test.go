// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package testhelpers

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	useDocker = flag.Bool("use-docker", true, "start database using a docker container")
	database  = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
)

func TestPGConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db, err := pgx.Connect(context.Background(), pgConnectURL(t, defaultDB))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(context.Background())
	var res int
	err = db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Errorf("Res is not 1 but %d", res)
	}
}

func TestWithDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	WithDB(t, *database, func(db *pgxpool.Pool, t *testing.T, connectURL string) {
		var res int
		err := db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
		if err != nil {
			t.Fatal(err)
		}
		if res != 1 {
			t.Errorf("Res is not 1 but %d", res)
		}
	})
}

func TestMain(m *testing.M) {
	flag.Parse()
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		container, err := StartPGContainer(ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer func() {
			err := container.Terminate(ctx)
			if err != nil {
				panic(err)
			}
		}()
	}
	code := m.Run()
	os.Exit(code)
}
