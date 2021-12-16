// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/log"
)

func init() {
	err := log.Init(log.Config{
		Level: "debug",
	})
	if err != nil {
		panic(err)
	}
}

type flagValues struct {
	First  string
	Second string
	Third  string
	Fourth int
}

func TestParseEnv(t *testing.T) {
	testCases := []struct {
		name       string
		prefixes   []string
		env        map[string]string
		args       []string
		flagValues flagValues
		err        error
	}{
		{
			name: "No env variables set",
		},
		{
			name:     "single prefix",
			prefixes: []string{"PREFIX"},
			env: map[string]string{
				"PREFIX_FIRST": "first value",
			},
			flagValues: flagValues{
				First: "first value",
			},
		},
		{
			name:     "cli args have precedence",
			prefixes: []string{"PREFIX"},
			env: map[string]string{
				"PREFIX_FIRST": "first value",
			},
			args: []string{
				"-first", "other value",
			},
			flagValues: flagValues{
				First: "other value",
			},
		},
		{
			name:     "multiple prefixes, first prefix parsed gets precedence",
			prefixes: []string{"FIRST_PREFIX", "SECOND_PREFIX"},
			env: map[string]string{
				"FIRST_PREFIX_FIRST":  "first value",
				"SECOND_PREFIX_FIRST": "second value",
			},
			flagValues: flagValues{
				First: "first value",
			},
		},
		{
			name:     "error parsing env variables, wrong type",
			prefixes: []string{"PREFIX"},
			env: map[string]string{
				"PREFIX_FOURTH": "foobar",
			},
			err: fmt.Errorf(`error setting flag "fourth" from env variable "PREFIX_FOURTH": parse error`),
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			os.Clearenv()
			for name, value := range c.env {
				if err := os.Setenv(name, value); err != nil {
					t.Fatalf("unexpected error when setting env var: name %s value %s error %s", name, value, err)
				}
			}

			fs := flag.NewFlagSet("test flag set", flag.ContinueOnError)
			values := flagValues{}

			fs.StringVar(&values.First, "first", "", "")
			fs.StringVar(&values.Second, "second", "", "")
			fs.StringVar(&values.Third, "third", "", "")
			fs.IntVar(&values.Fourth, "fourth", 0, "")

			for _, prefix := range c.prefixes {
				if err := ParseEnv(prefix, fs); err != nil {
					if c.err == nil {
						t.Fatalf("unexpected error while parsing env variables: %s", err)
					}
					if c.err.Error() != err.Error() {
						t.Fatalf("unexpected error while parsing flags:\ngot\n%s\nwanted\n%s\n", err, c.err)
					}
					return
				}
			}
			if err := fs.Parse(c.args); err != nil {
				t.Fatalf("unexpected error while parsing flags: %s", err)
			}

			if !reflect.DeepEqual(values, c.flagValues) {
				t.Fatalf("Unexpected flag values set\nwanted:\n%+v\ngot:\n%+v\n", c.flagValues, values)
			}
		})
	}

}

func TestAddAlias(t *testing.T) {
	aliases := map[string][]string{
		"first_flag":  {"first_flag_alias", "first_flag_another_alias"},
		"second_flag": {"second_flag_alias"},
	}

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Bool("first_flag", true, "First flag desc")
	fs.String("second_flag", "empty", "Second flag desc")

	descSuffix := "Test description."

	AddAliases(fs, aliases, descSuffix)

	ff := fs.Lookup("first_flag")
	sf := fs.Lookup("second_flag")

	require.NotNil(t, ff)
	require.NotNil(t, sf)

	alias := fs.Lookup("first_flag_alias")
	require.NotNil(t, alias)
	require.Equal(t, ff.Value, alias.Value)
	require.Equal(t, ff.DefValue, alias.DefValue)
	require.Equal(t, alias.Usage, fmt.Sprintf(aliasDescTemplate+descSuffix, ff.Name))

	alias = fs.Lookup("first_flag_another_alias")
	require.NotNil(t, alias)
	require.Equal(t, ff.Value, alias.Value)
	require.Equal(t, ff.DefValue, alias.DefValue)
	require.Equal(t, alias.Usage, fmt.Sprintf(aliasDescTemplate+descSuffix, ff.Name))

	alias = fs.Lookup("second_flag_alias")
	require.NotNil(t, alias)
	require.Equal(t, sf.Value, alias.Value)
	require.Equal(t, sf.DefValue, alias.DefValue)
	require.Equal(t, alias.Usage, fmt.Sprintf(aliasDescTemplate+descSuffix, sf.Name))

}
