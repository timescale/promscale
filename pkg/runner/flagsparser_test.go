// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
//
// https://github.com/peterbourgon/ff/blob/7a9748fa77b6d2664e5553540a9cb69d31978815/ffyaml/ffyaml_test.go
package runner

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/peterbourgon/ff/v3"
	"github.com/peterbourgon/ff/v3/fftest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	t.Parallel()

	for _, testcase := range []struct {
		vars func(*flag.FlagSet) *fftest.Vars
		name string
		file string
		miss bool // AllowMissingConfigFiles
		want fftest.Vars
	}{
		{
			name: "empty",
			file: "testdata/empty.yaml",
			want: fftest.Vars{},
		},
		{
			name: "basic KV pairs",
			file: "testdata/basic.yaml",
			want: fftest.Vars{S: "hello", I: 10, B: true, D: 5 * time.Second, F: 3.14},
		},
		{
			name: "invalid prefix",
			file: "testdata/invalid_prefix.yaml",
			want: fftest.Vars{WantParseErrorString: "found character that cannot start any token"},
		},
		{
			vars: fftest.NonzeroDefaultVars,
			name: "no value for s",
			file: "testdata/no_value_s.yaml",
			want: fftest.Vars{S: "", I: 123, F: 9.99, B: true, D: 3 * time.Hour},
		},
		{
			vars: fftest.NonzeroDefaultVars,
			name: "no value for i",
			file: "testdata/no_value_i.yaml",
			want: fftest.Vars{WantParseErrorString: "parse error"},
		},
		{
			name: "basic arrays",
			file: "testdata/basic_array.yaml",
			want: fftest.Vars{S: "c", X: []string{"a", "b", "c"}},
		},
		{
			name: "multiline arrays",
			file: "testdata/multi_line_array.yaml",
			want: fftest.Vars{S: "c", X: []string{"d", "e", "f"}},
		},
		{
			name: "line break arrays",
			file: "testdata/line_break_array.yaml",
			want: fftest.Vars{X: []string{"first string", "second string", "third"}},
		},
		{
			name: "unquoted strings in arrays",
			file: "testdata/unquoted_string_array.yaml",
			want: fftest.Vars{X: []string{"one", "two", "three"}},
		},
		{
			name: "missing config file allowed",
			file: "testdata/this_file_does_not_exist.yaml",
			miss: true,
			want: fftest.Vars{},
		},
		{
			name: "missing config file not allowed",
			file: "testdata/this_file_does_not_exist.yaml",
			miss: false,
			want: fftest.Vars{WantParseErrorIs: os.ErrNotExist},
		},
		{
			name: "dataset config as string",
			file: "testdata/dataset_string.yaml",
			want: fftest.Vars{X: []string{"one", "two", "three"}},
		},
		{
			name: "dataset config as map",
			file: "testdata/dataset_map.yaml",
			want: fftest.Vars{X: []string{"one", "two", "three"}},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			if testcase.vars == nil {
				testcase.vars = fftest.DefaultVars
			}
			fs := flag.NewFlagSet("fftest", flag.ContinueOnError)
			vars := testcase.vars(fs)
			vars.ParseError = ff.Parse(fs, []string{},
				ff.WithConfigFile(testcase.file),
				ff.WithConfigFileParser(Parser),
				ff.WithAllowMissingConfigFile(testcase.miss),
			)
			fftest.Compare(t, &testcase.want, vars)
		})
	}
}

func TestParserDataset(t *testing.T) {
	t.Parallel()

	for _, testcase := range []struct {
		name string
		file string
	}{
		{
			name: "dataset config as string",
			file: "testdata/dataset_string.yaml",
		},
		{
			name: "dataset config as map",
			file: "testdata/dataset_map.yaml",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			cfg := Config{}
			fs := flag.NewFlagSet("fftest", flag.ContinueOnError)
			fs.StringVar(&cfg.DatasetConfig, "startup.dataset.config", "", "Dataset configuration in YAML format for Promscale. It is used for setting various dataset configuration like default metric chunk interval")
			err := ff.Parse(fs, []string{},
				ff.WithConfigFile(testcase.file),
				ff.WithConfigFileParser(Parser),
				ff.WithAllowMissingConfigFile(false),
			)
			require.NoError(t, err)
			assert.Equal(t, "metrics:\n  default_chunk_interval: 6h\n", cfg.DatasetConfig)
		})
	}
}
