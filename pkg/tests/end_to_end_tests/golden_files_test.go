// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package end_to_end_tests

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sergi/go-diff/diffmatchpatch"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var outputDifferWithoutTimescale = map[string]bool{"info_view": true}
var outputDifferWithMultinode = map[string]bool{"views": true, "info_view": true}

var requiresSingleNode = map[string]bool{"support": true}

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

			if *useMultinode && requiresSingleNode[base] {
				return
			}

			i, err := pgContainer.Exec(context.Background(), []string{"bash", "-c", "psql -U postgres -d " + *testDatabase + " -f /testdata/sql/" + base + ".sql &> /testdata/out/" + base + ".out"})
			if err != nil {
				t.Fatal(err)
			}

			actualFile := filepath.Join(pgContainerTestDataDir, "out", base+".out")

			actual, err := os.ReadFile(actualFile)
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

				msg, err := io.ReadAll(rc)
				if err != nil {
					t.Fatal(err)
				}
				t.Log(string(msg))
			}

			var suffix string
			switch {
			case outputDifferWithMultinode[base] && *useMultinode:
				suffix = "-timescaledb-multinode"
			case outputDifferWithoutTimescale[base]:
				suffix = "-timescaledb"
			default:
				suffix = ""
			}
			expectedFile := filepath.Join("../testdata/expected/", base+suffix+".out")

			if *updateGoldenFiles {
				err = copyFile(actualFile, expectedFile)
				if err != nil {
					t.Fatal(err)
				}
			}

			expected, err := os.ReadFile(expectedFile)
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
