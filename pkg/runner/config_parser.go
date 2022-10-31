// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package runner

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"github.com/timescale/promscale/pkg/dataset"
)

// unmarshalRule defines that the subtree located on the `key` of the Viper
// config should be unmarshal into the `target`.
type unmarshalRule struct {
	key    string
	target interface{}
}

type parserOption func(*parserConfig)

type parserConfig struct {
	ignoreUndefined bool
	unmarshalRules  []unmarshalRule
}

// parse the flags in the flag set from the provided (presumably commandline)
// args.
//
// This method replicates the behaviour we had with `ff.Parse`, with the
// difference that this one uses viper to load the yaml configuration file.
//
// By design the ff package doesn't allow flag subsets or nested maps inside
// the config yaml and all config options must be explicitely defined as
// `flag.Flag`. This limits the ability to specify more dynamic config options
// like lists or maps of user defined rules (Ex: metric rollups).
//
// Viper provides the flexibility we require for parsing the config file, and
// unmarshaling directly into more complex datastructures. To define how to
// unmarshal a subtree of the configuration into a datastructure via
// unmarshalRule:
//
// ```
// withUnmarshalRules(
//   []unmarshalRule{{"startup.dataset", &dataset.Config}}
// )
// ```
//
// Given a config file that contains the subtree:
//
// ```
// startup:
//   dataset:
//     metrics:
//       ...
//     traces:
//       ...
// ```
//
// Viper can access a nested field by passing a `.` delimited path of keys.
// This change also allows us to format our configuration files to use
// yaml mappings instead of long key names with `.`, thus maintaining
// backwards compatibility:
//
// ```
// web.listen-address: localhost:9201
// web.auth.password: my-password
// web.auth.username: promscale
// ```
//
// Can be rewritten as:
//
// ```
// web:
//   listen-address: localhost:9201
//   auth:
//     password: my-password
//     username: promscale
// ```
//
// In case of conflict the variable that matches the delimited key path will
// take precedence (https://github.com/spf13/viper#accessing-nested-keys). In
// the following example the returned value would be `localhost:9201`:
//
// ```
// web.listen-address: localhost:9201
// web:
//   listen-address: htto://not.going.to.be.used:9201
// ```
//
// Migrating our codebase to take advantage of all the features of Viper will
// require some extra effort, and might not bring extra benefits since we
// already consolidate commandline arguments with environment variables. This
// approach gives us the features we need without the need of extensive
// modification.
func parse(fSet *flag.FlagSet, args []string, options ...parserOption) error {

	cfg := getParserConfig(options)

	// First priority: commandline flags (explicit user preference).
	if err := fSet.Parse(args); err != nil {
		return fmt.Errorf("error parsing commandline args: %w", err)
	}

	configFile, err := configFileNameFromFlags(fSet)

	// This error won't be triggered unless the line that defines the config
	// flag is deleted.
	if err != nil {
		return err
	}

	v, err := newViperConfig(configFile)

	if err != nil {
		// If the file can't be found, we don't return the error. This is the
		// same behaviour we had with `ff.WithAllowMissingConfigFile(true)`.
		var e *fs.PathError
		if errors.As(err, &e) {
			return nil
		}
		return fmt.Errorf("couldn't load config file %s: %w", configFile, err)
	}

	if !cfg.ignoreUndefined {
		err := checkUndefinedFlags(v, fSet, cfg.unmarshalRules)
		if err != nil {
			return err
		}
	}

	err = setFlagsWithConfigFileValues(fSet, v)

	if err != nil {
		return fmt.Errorf(
			"Couldn't apply the configuration from file %s: %w",
			configFile,
			err,
		)
	}

	err = applyUnmarshalRules(v, cfg.unmarshalRules)

	return err
}

// withIgnoreUndefined tells parse to ignore undefined flags that it encounters
// in config files. By default, if parse encounters an undefined flag in a
// config file, it will return an error. Note that this setting does not apply
// to undefined flags passed as arguments.
func withIgnoreUndefined(ignore bool) parserOption {
	return func(c *parserConfig) {
		c.ignoreUndefined = ignore
	}
}

func withUnmarshalRules(r []unmarshalRule) parserOption {
	return func(c *parserConfig) {
		c.unmarshalRules = r
	}
}
func newViperConfig(configFile string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	v.SetConfigType("yaml")
	err := v.ReadInConfig()
	return v, err
}

func configFileNameFromFlags(fSet *flag.FlagSet) (string, error) {
	f := fSet.Lookup(configFileFlagName)
	if f == nil {
		return "", fmt.Errorf("config file flag not defined")
	}

	return f.Value.String(), nil
}

func getFlagsThatHaveBeenSet(fSet *flag.FlagSet) map[string]struct{} {
	provided := map[string]struct{}{}
	fSet.Visit(func(f *flag.Flag) {
		provided[f.Name] = struct{}{}
	})
	return provided
}

// checkUndefinedFlags returns an error if it founds at least one key in the
// Viper registry that's neither defined in the FlagSet nor is part of an
// unmarshalRule.
func checkUndefinedFlags(
	v *viper.Viper,
	fSet *flag.FlagSet,
	rules []unmarshalRule,
) error {

	for _, k := range v.AllKeys() {
		// If it's part of a unmarshalRule we do nothing
		isUnmarshalRule := false
		for _, r := range rules {
			if strings.HasPrefix(k, r.key) {
				isUnmarshalRule = true
				break
			}
		}
		if isUnmarshalRule {
			continue
		}

		if f := fSet.Lookup(k); f == nil {
			return fmt.Errorf("config file flag %q not defined in flag set", k)
		}
	}
	return nil
}

// setFlagsWithConfigFileValues sets the value of the unset flags from the
// FlagSet with the matching entries in the Viper registry.
//
// Viper contains the values from the config file.
//
// Only flags that have not been set are changed. This is to respect the
// precedence between env vars, command line args and config file.
func setFlagsWithConfigFileValues(
	fSet *flag.FlagSet,
	v *viper.Viper,
) error {

	// These are the flags that have matched against the given `args`. These
	// have already consolidated commandline arguments (via FlagSet.Parse) and
	// environment variables (via `pkg/util/util.go:ParseEnv`).
	alreadySetFlags := getFlagsThatHaveBeenSet(fSet)

	var visitErr error
	fSet.VisitAll(func(f *flag.Flag) {
		if visitErr != nil {
			return
		}
		if _, ok := alreadySetFlags[f.Name]; ok {
			return
		}
		if !v.IsSet(f.Name) {
			return
		}
		val := v.GetString(f.Name)
		err := fSet.Set(f.Name, val)
		if err != nil {
			visitErr = err
		}
	})

	return visitErr
}

func applyUnmarshalRules(v *viper.Viper, unmarshalRules []unmarshalRule) error {
	for _, rule := range unmarshalRules {
		s := v.Sub(rule.key)
		if s == nil {
			continue
		}
		err := s.Unmarshal(
			rule.target,
			viper.DecodeHook(
				mapstructure.ComposeDecodeHookFunc(
					dataset.StringToDayDurationHookFunc(),
					mapstructure.StringToTimeDurationHookFunc(),
					mapstructure.StringToSliceHookFunc(","),
				),
			),
		)
		if err != nil {
			return fmt.Errorf(
				"failed to apply unmarshal rule for config file item %s: %w",
				rule.key,
				err,
			)
		}
	}
	return nil
}

func getParserConfig(options []parserOption) parserConfig {
	cfg := parserConfig{}
	for _, option := range options {
		option(&cfg)
	}
	return cfg
}
