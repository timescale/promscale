package pgmodel

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
)

var (
	database = flag.String("database", "migrate_test", "database to run integration tests on")
)

const (
	expectedVersion = 1
)

func TestMigrate(t *testing.T) {
	withDB(t, func(db *pgx.Conn, t *testing.T) {
		var version int64
		var dirty bool
		err := db.QueryRow(context.Background(), "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty)
		if err != nil {
			t.Fatal(err)
		}
		if version != expectedVersion {
			t.Errorf("Version unexpected:\ngot\n%d\nwanted\n%d", version, expectedVersion)
		}
		if dirty {
			t.Error("Dirty is true")
		}

	})
}

func withDB(t *testing.T, f func(db *pgx.Conn, t *testing.T)) {
	db := dbSetup(t)
	performMigrate(t)
	defer func() {
		if db != nil {
			db.Close(context.Background())
		}
	}()
	f(db, t)
}

func performMigrate(t *testing.T) {
	dbStd, err := sql.Open("pgx", fmt.Sprintf("postgres://postgres@localhost/%s?sslmode=disable", *database))
	if err != nil {
		t.Fatal(err)
	}
	err = Migrate(dbStd)
	if err != nil {
		t.Fatal(err)
	}
}

func dbSetup(t *testing.T) *pgx.Conn {
	flag.Parse()
	if len(*database) == 0 {
		t.Skip()
	}
	db, err := pgx.Connect(context.Background(), "host=localhost user=postgres sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", *database))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", *database))
	if err != nil {
		t.Fatal(err)
	}

	db, err = pgx.Connect(context.Background(), fmt.Sprintf("host=localhost user=postgres database=%s sslmode=disable", *database))
	if err != nil {
		t.Fatal(err)
	}
	return db
}
