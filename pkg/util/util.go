// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

const PromNamespace = "promscale"

// ParseEnv takes a prefix string p and *flag.FlagSet. Each flag
// in the FlagSet is exposed as an upper case environment variable
// prefixed with p. Any flag that was not explicitly set by a user
// is updated to the environment variable, if set.
//
// Note: when run with multiple times with different prefixes on the
// same FlagSet, precedence will get values set with prefix which is
// parsed first.
func ParseEnv(p string, fs *flag.FlagSet) error {
	var err error
	// Build a map of explicitly set flags.
	set := make(map[string]struct{})
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = struct{}{}
	})

	fs.VisitAll(func(f *flag.Flag) {
		// If an error occured while processing other flags, abort.
		if err != nil {
			return
		}
		// Create an env var name
		// based on the supplied prefix.
		envVar := GetEnvVarName(p, f.Name)

		// Update the Flag.Value if the
		// env var is non "".
		if val := os.Getenv(envVar); val != "" {
			// Update the value if it hasn't
			// already been set.
			if _, defined := set[f.Name]; !defined {
				if err = fs.Set(f.Name, val); err != nil {
					err = fmt.Errorf(`error setting flag "%s" from env variable "%s": %w`,
						f.Name,
						envVar,
						err)
					return
				}
			}
		}

		// Append the env var to the
		// Flag.Usage field.
		f.Usage = fmt.Sprintf("%s [%s]", f.Usage, envVar)
	})

	return err
}

// GetEnvVarName returns the name of the environment variable used
// for setting the configuration flag based on a prefix and flag name.
func GetEnvVarName(prefix, fName string) (envVar string) {
	envVar = fmt.Sprintf("%s_%s", prefix, strings.ToUpper(fName))
	envVar = strings.ReplaceAll(envVar, "-", "_")
	return strings.ReplaceAll(envVar, ".", "_")
}

func LabelToPrompbLabels(l labels.Labels) []prompb.Label {
	if len(l) == 0 {
		return []prompb.Label{}
	}
	lbls := make([]prompb.Label, len(l))
	for i := range l {
		lbls[i].Name = l[i].Name
		lbls[i].Value = l[i].Value
	}
	return lbls
}

func IsTimescaleDBInstalled(conn pgxconn.PgxConn) bool {
	var installed bool
	err := conn.QueryRow(context.Background(), `SELECT count(*) > 0 FROM pg_extension where extname = 'timescaledb'`).Scan(&installed)
	if err != nil {
		log.Error("msg", "error checking if TimescaleDB is installed", "err", err.Error())
		return false
	}
	return installed
}

// Pointer returns a pointer to the given variabel. Useful in tests for
// primitive value pointer arguments.
func Pointer[T any](x T) *T {
	return &x
}
