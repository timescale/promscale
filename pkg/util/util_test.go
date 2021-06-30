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
