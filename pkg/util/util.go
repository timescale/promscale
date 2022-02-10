// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

const (
	PromNamespace     = "promscale"
	aliasDescTemplate = "Alias for -%s flag. "
)

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
		envVar := fmt.Sprintf("%s_%s", p, strings.ToUpper(f.Name))
		envVar = strings.Replace(envVar, "-", "_", -1)
		envVar = strings.Replace(envVar, ".", "_", -1)

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

func AddAliases(fs *flag.FlagSet, aliases map[string][]string, descSuffix string) {
	aliasDescFormat := aliasDescTemplate + descSuffix
	set := false
	fs.VisitAll(func(f *flag.Flag) {
		if flagAliases, ok := aliases[f.Name]; ok {
			set = false
			for _, alias := range flagAliases {
				set = true
				fs.Var(f.Value, alias, fmt.Sprintf(aliasDescFormat, f.Name))
			}

			if !set {
				panic(fmt.Sprintf("trying to set an flag alias for a flag that is missing: %s", f.Name))
			}
		}
	})
}
