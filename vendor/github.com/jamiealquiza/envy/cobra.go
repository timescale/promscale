package envy

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// CobraConfig holds configurations for calls
// to ParseCobra.
type CobraConfig struct {
	// The environment variable prefix.
	Prefix string
	// Expose flags for child commands.
	Recursive bool
	// Whether to expose flags for persistent FlagSets.
	Persistent bool
}

// ParseCobra takes a *cobra.Command and exposes environment variables
// for all local flags in the command FlagSet in the form of PREFIX_FLAGNAME
// where PREFIX is set to that of CobraConfig.Prefix. Environment variables are
// exposed for persistent flags if the CobraConfig.Persistent is set to true.
// If CobraConfig.Recursive is set to true, all child command FlagSets will
// have environment variables exposed in the form of PREFIX_SUBCOMMMAND_FLAGNAME.
func ParseCobra(c *cobra.Command, cfg CobraConfig) {
	// Check if this is the root command.
	switch c.Root() == c {
	case false:
		// If not, append subcommand names to the prefix.
		cfg.Prefix = fmt.Sprintf("%s_%s", cfg.Prefix, strings.ToUpper(c.Name()))
	case true && cfg.Persistent:
		// If this is the root command, update the
		// persistent FlagSet, if configured.
		updateCobra(cfg.Prefix, c.PersistentFlags())
	}

	// Update the current command local FlagSet.
	updateCobra(cfg.Prefix, c.Flags())

	// Recursively update child commands.
	if cfg.Recursive {
		for _, child := range c.Commands() {
			if child.Name() == "help" {
				continue
			}

			ParseCobra(child, cfg)
		}
	}
}

func updateCobra(p string, fs *pflag.FlagSet) {
	// Build a map of explicitly set flags.
	set := map[string]interface{}{}
	fs.Visit(func(f *pflag.Flag) {
		set[f.Name] = nil
	})

	fs.VisitAll(func(f *pflag.Flag) {
		if f.Name == "help" {
			return
		}

		// Create an env var name
		// based on the supplied prefix.
		envVar := fmt.Sprintf("%s_%s", p, strings.ToUpper(f.Name))
		envVar = strings.Replace(envVar, "-", "_", -1)

		// Update the Flag.Value if the
		// env var is non "".
		if val := os.Getenv(envVar); val != "" {
			// Update the value if it hasn't
			// already been set.
			if _, defined := set[f.Name]; !defined {
				fs.Set(f.Name, val)
			}
		}

		// Append the env var to the
		// Flag.Usage field.
		f.Usage = fmt.Sprintf("%s [%s]", f.Usage, envVar)
	})
}
