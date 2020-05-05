// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v4/pgxpool"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func TestSQLGoldenFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		files, err := filepath.Glob("testdata/sql/*")
		if err != nil {
			t.Fatal(err)
		}

		for _, file := range files {
			base := filepath.Base(file)
			base = strings.TrimSuffix(base, filepath.Ext(base))
			i, err := pgContainer.Exec(context.Background(), []string{"bash", "-c", "psql -U postgres -d " + *testDatabase + " -f /testdata/sql/" + base + ".sql &> /testdata/out/" + base + ".out"})
			if err != nil {
				t.Fatal(err)
			}

			if i != 0 {
				/* on psql failure print the logs */
				rc, err := pgContainer.Logs(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer rc.Close()

				msg, err := ioutil.ReadAll(rc)
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(msg))
			}

			expectedFile := filepath.Join("testdata/expected/", base+".out")
			actualFile := filepath.Join(pgContainerTestDataDir, "out", base+".out")

			if *updateGoldenFiles {
				err = copyFile(actualFile, expectedFile)
				if err != nil {
					t.Fatal(err)
				}
			}

			expected, err := ioutil.ReadFile(expectedFile)
			if err != nil {
				t.Fatal(err)
			}

			actual, err := ioutil.ReadFile(actualFile)
			if err != nil {
				t.Fatal(err)
			}

			if string(expected) != string(actual) {
				t.Fatalf("Golden file does not match result: diff %s %s", expectedFile, actualFile)
			}
		}
	})
}
