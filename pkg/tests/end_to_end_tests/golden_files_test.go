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
	"github.com/sergi/go-diff/diffmatchpatch"

	_ "github.com/jackc/pgx/v4/stdlib"
)

var outputDifferWithoutTimescale = map[string]bool{"info_view": true}
var outputDifferWithMultinode = map[string]bool{"views": true, "info_view": true, "support": true}
var outputDifferWithExtension = map[string]bool{"support": true}
var requiresTimescaleDB = map[string]bool{"views": true, "info_view": true, "support": true}

func TestSQLGoldenFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	files, err := filepath.Glob("../testdata/sql/*")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) <= 0 {
		t.Fatal("No sql files found")
	}

	for _, file := range files {
		withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
			base := filepath.Base(file)
			base = strings.TrimSuffix(base, filepath.Ext(base))

			if !*useTimescaleDB && requiresTimescaleDB[base] {
				return
			}

			i, err := pgContainer.Exec(context.Background(), []string{"bash", "-c", "psql -U postgres -d " + *testDatabase + " -f /testdata/sql/" + base + ".sql &> /testdata/out/" + base + ".out"})
			if err != nil {
				t.Fatal(err)
			}

			actualFile := filepath.Join(pgContainerTestDataDir, "out", base+".out")

			actual, err := ioutil.ReadFile(actualFile)
			if err != nil {
				t.Fatal(err)
			}

			if i != 0 {
				t.Logf("Failure in test %s", base)
				t.Log(string(actual))

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

			mod := ""
			if *useTimescale2 {
				mod = "-2"
			}

			expectedFile := filepath.Join("../testdata/expected/", base+mod+".out")
			if outputDifferWithoutTimescale[base] {
				if *useTimescaleDB {
					expectedFile = filepath.Join("../testdata/expected/", base+"-timescaledb"+mod+".out")
				} else {
					expectedFile = filepath.Join("../testdata/expected/", base+"-postgres.out")
				}
			}
			if outputDifferWithMultinode[base] && *useMultinode {
				expectedFile = filepath.Join("../testdata/expected/", base+"-timescaledb"+mod+"-multinode.out")
			}
			if outputDifferWithExtension[base] && !*useExtension {
				expectedFile = filepath.Join("../testdata/expected/", base+mod+"-noextension.out")
			}

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

			if string(expected) != string(actual) {
				dmp := diffmatchpatch.New()
				fileAdmp, fileBdmp, dmpStrings := dmp.DiffLinesToChars(string(expected), string(actual))
				diffs := dmp.DiffMain(fileAdmp, fileBdmp, false)
				diffs = dmp.DiffCharsToLines(diffs, dmpStrings)
				diffs = dmp.DiffCleanupSemantic(diffs)
				t.Errorf("Golden file does not match result: diff %s %s\n\n%v", expectedFile, actualFile, dmp.DiffPrettyText(diffs))
			}

		})
	}
}
