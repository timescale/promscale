// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel"
	"github.com/timescale/promscale/pkg/version"

	_ "github.com/jackc/pgx/v4/stdlib"

	. "github.com/timescale/promscale/pkg/pgmodel"
)

var (
	testDatabase      = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	updateGoldenFiles = flag.Bool("update", false, "update the golden files of this test")
	useDocker         = flag.Bool("use-docker", true, "start database using a docker container")
	useExtension      = flag.Bool("use-extension", true, "use the promscale extension")
	printLogs         = flag.Bool("print-logs", false, "print TimescaleDB logs")
	extendedTest      = flag.Bool("extended-test", false, "run extended testing dataset and PromQL queries")

	pgContainer            testcontainers.Container
	pgContainerTestDataDir string
)

func TestMain(m *testing.M) {
	var code int
	func() {
		flag.Parse()
		ctx := context.Background()
		_ = log.Init(log.Config{
			Level: "debug",
		})
		if !testing.Short() {
			var err error

			if *useDocker {
				pgContainerTestDataDir = generatePGTestDirFiles()

				pgContainer, err = testhelpers.StartPGContainer(ctx, *useExtension, pgContainerTestDataDir, *printLogs)
				if err != nil {
					fmt.Println("Error setting up container", err)
					os.Exit(1)
				}
			}

			storagePath, err := generatePrometheusWALFile()
			if err != nil {
				fmt.Println("Error creating WAL file", err)
				os.Exit(1)
			}

			promCont, err := testhelpers.StartPromContainer(storagePath, ctx)
			if err != nil {
				fmt.Println("Error setting up container", err)
				os.Exit(1)
			}
			defer func() {
				if err != nil {
					panic(err)
				}

				if *useDocker {
					if *printLogs {
						_ = pgContainer.StopLogProducer()
					}

					_ = pgContainer.Terminate(ctx)
					pgContainer = nil
				}
				err = promCont.Terminate(ctx)
				if err != nil {
					panic(err)
				}
			}()
		}
		code = m.Run()
	}()
	os.Exit(code)
}

func withDB(t testing.TB, DBName string, f func(db *pgxpool.Pool, t testing.TB)) {
	testhelpers.WithDB(t, DBName, testhelpers.NoSuperuser, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
		performMigrate(t, connectURL)

		// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
		db, err := pgxpool.Connect(context.Background(), connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		f(db, t)
	})
}

func performMigrate(t testing.TB, connectURL string) {
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
	err = Migrate(conn.Conn(), pgmodel.VersionInfo{Version: version.Version, CommitHash: "azxtestcommit"})
	if err != nil {
		t.Fatal(err)
	}
}

func generatePGTestDirFiles() string {
	tmpDir, err := testhelpers.TempDir("testdata")
	if err != nil {
		log.Fatal(err)
	}

	err = os.Mkdir(filepath.Join(tmpDir, "sql"), 0777)
	if err != nil {
		log.Fatal(err)
	}
	err = os.Mkdir(filepath.Join(tmpDir, "out"), 0777)
	if err != nil {
		log.Fatal(err)
	}

	files, err := filepath.Glob("testdata/sql/*")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		err = copyFile(file, filepath.Join(tmpDir, "sql", filepath.Base(file)))
		if err != nil {
			log.Fatal(err)
		}
	}
	return tmpDir
}

func copyFile(src string, dest string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	newFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer newFile.Close()

	_, err = io.Copy(newFile, sourceFile)
	if err != nil {
		return err
	}
	return nil
}
