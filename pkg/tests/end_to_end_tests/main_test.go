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
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/timescale/promscale/pkg/internal/testhelpers"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/prompb"
	"github.com/timescale/promscale/pkg/tests/common"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

var (
	testDatabase          = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	updateGoldenFiles     = flag.Bool("update", false, "update the golden files of this test")
	useDocker             = flag.Bool("use-docker", true, "start database using a docker container")
	useExtension          = flag.Bool("use-extension", true, "use the promscale extension")
	useTimescaleDB        = flag.Bool("use-timescaledb", true, "use TimescaleDB")
	useTimescale2         = flag.Bool("use-timescale2", true, "use TimescaleDB 2.0")
	useTimescaleOSS       = flag.Bool("use-timescaledb-oss", false, "use TimescaleDB-OSS latest")
	postgresVersion       = flag.Int("postgres-version-major", 13, "Major version of Postgres")
	useMultinode          = flag.Bool("use-multinode", false, "use TimescaleDB 2.0 Multinode")
	useTimescaleDBNightly = flag.Bool("use-timescaledb-nightly", false, "use TimescaleDB nightly images")
	printLogs             = flag.Bool("print-logs", false, "print TimescaleDB logs")
	extendedTest          = flag.Bool("extended-test", false, "run extended testing dataset and PromQL queries")
	logLevel              = flag.String("log-level", "debug", "Logging level")

	pgContainer            testcontainers.Container
	pgContainerTestDataDir string
	promContainer          testcontainers.Container
	promHost               string
	promPort               nat.Port
	// We need a different set of environment for testing exemplars. This is because behaviour of exemplars and that of samples
	// are very different in Prometheus, in nutshell, exemplars are WAL only. The moment a block is created, exemplars are lost.
	// Hence, we need an isolated environment for testing exemplars. In the future, once exemplars become block compatible,
	// we can remove this separation.
	promExemplarContainer testcontainers.Container
	promExemplarHost      string
	promExemplarPort      nat.Port
	// extensionState expects setExtensionState() to be called before using its value.
	extensionState testhelpers.ExtensionState
	// timeseriesWithExemplars is a singleton instance for testing generated timeseries for exemplars based E2E tests.
	// The called must ensure to use by call/copy only.
	timeseriesWithExemplars []prompb.TimeSeries
)

func init() {
	tput.InitWatcher(time.Second)
}

// setExtensionState sets the value of extensionState based on the input flags.
func setExtensionState() {
	switch *postgresVersion {
	case 12:
		extensionState.UsePG12()
	case 13:
	default:
		panic("Unknown Postgres version")
	}

	if *useExtension {
		extensionState.UsePromscale()
	}

	if *useTimescaleDB {
		extensionState.UseTimescaleDB()
	}

	if *useTimescale2 {
		*useTimescaleDB = true
		extensionState.UseTimescaleDB2()
	}

	if *useTimescaleDBNightly {
		extensionState.UseTimescaleNightly()
	}

	if *useMultinode {
		extensionState.UseMultinode()
		*useTimescaleDB = true
		*useTimescale2 = true
	}

	if *useTimescaleOSS {
		extensionState.UseTimescaleDBOSS()
		*useTimescaleDB = true
		*useTimescale2 = true
	}
}

func TestMain(m *testing.M) {
	var code int
	func() {
		flag.Parse()
		ctx := context.Background()
		err := log.Init(log.Config{
			Level: *logLevel,
		})
		if err != nil {
			panic(err)
		}
		if !testing.Short() && *useDocker {
			var err error

			var closer io.Closer
			setExtensionState()

			pgContainerTestDataDir = generatePGTestDirFiles()

			pgContainer, closer, err = testhelpers.StartPGContainer(
				ctx,
				extensionState,
				pgContainerTestDataDir,
				*printLogs,
			)
			if err != nil {
				fmt.Println("Error setting up container", err)
				os.Exit(1)
			}

			_, storagePath, err := generatePrometheusWAL(false)
			if err != nil {
				fmt.Println("Error creating WAL file", err)
				os.Exit(1)
			}

			promContainer, promHost, promPort, err = testhelpers.StartPromContainer(storagePath, ctx)
			if err != nil {
				fmt.Println("Error setting up container", err)
				os.Exit(1)
			}

			var storageExemplarPath string
			timeseriesWithExemplars, storageExemplarPath, err = generatePrometheusWAL(true)
			if err != nil {
				fmt.Println("Error creating WAL with exemplar file", err)
				os.Exit(1)
			}

			promExemplarContainer, promExemplarHost, promExemplarPort, err = testhelpers.StartPromContainer(storageExemplarPath, ctx)
			defer func() {
				if err != nil {
					panic(err)
				}

				if closer != nil {
					_ = closer.Close()
				}
				err = promContainer.Terminate(ctx)
				if err != nil {
					panic(err)
				}
			}()
		}
		code = m.Run()
	}()
	os.Exit(code)
}

func attachDataNode2(t testing.TB, DBName string, connectURL string) {
	db, err := pgx.Connect(context.Background(), testhelpers.PgConnectURL(DBName, testhelpers.Superuser))
	if err != nil {
		t.Fatal(err)
	}
	err = testhelpers.AddDataNode2(db, DBName)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func addPromNode(t testing.TB, pool *pgxpool.Pool, attachExisting bool) {
	_, err := pool.Exec(context.Background(), "CALL add_prom_node('dn1', $1);", attachExisting)
	if err != nil {
		t.Fatal(err)
	}
}

func withDB(t testing.TB, DBName string, f func(db *pgxpool.Pool, t testing.TB)) {
	withDBAttachNode(t, DBName, true, nil, f)
}

/* When testing with multinode always add data node 2 after installing the extension, as that tests a strictly harder case */
func withDBAttachNode(t testing.TB, DBName string, attachExisting bool, beforeAddNode func(db *pgxpool.Pool, t testing.TB), afterAddNode func(db *pgxpool.Pool, t testing.TB)) {
	testhelpers.WithDB(t, DBName, testhelpers.NoSuperuser, true, extensionState, func(_ *pgxpool.Pool, t testing.TB, connectURL string) {
		common.PerformMigrate(t, connectURL, testhelpers.PgConnectURL(DBName, testhelpers.Superuser), *useTimescaleDB, *useExtension)

		if beforeAddNode != nil {
			if !*useMultinode {
				t.Fatal("Shouldn't be using beforeAddNode unless testing multinode")
			}
			func() {
				pool, err := pgxpool.Connect(context.Background(), connectURL)
				if err != nil {
					t.Fatal(err)
				}

				defer pool.Close()
				beforeAddNode(pool, t)
			}()

		}

		if *useMultinode {
			//add data 2 node /after/ install
			attachDataNode2(t, DBName, connectURL)
		}

		// need to get a new pool after the Migrate to catch any GUC changes made during Migrate
		pool, err := pgxpool.Connect(context.Background(), connectURL)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		if *useMultinode {
			//add prom node using the prom user (not superuser)
			addPromNode(t, pool, attachExisting)
		}

		afterAddNode(pool, t)
	})
}

func generatePGTestDirFiles() string {
	tmpDir, err := testhelpers.TempDir("testdata")
	if err != nil {
		log.Fatal(err)
	}

	err = os.Chmod(tmpDir, 0777)
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

	//Mkdir obeys umask. have to force
	err = os.Chmod(filepath.Join(tmpDir, "out"), 0777)
	if err != nil {
		log.Fatal(err)
	}

	files, err := filepath.Glob("../testdata/sql/*")
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

// newWriteRequestWithTs returns a new *prompb.WriteRequest from the pool and applies ts to it if ts is not nil.
func newWriteRequestWithTs(ts []prompb.TimeSeries) *prompb.WriteRequest {
	return common.NewWriteRequestWithTs(ts)
}
