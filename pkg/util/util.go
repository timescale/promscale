// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	PromNamespace              = "promscale"
	maskPasswordReplaceString1 = "password=$1'****'"
	maskPasswordReplaceString2 = "password:$1****$3"
	/* #nosec */
	maskPasswordReplaceString3 = "postgres:$1****"
)

var (
	maskPasswordRegex1 = regexp.MustCompile(`[p|P]assword=(\s*?)'([^']+?)'`)
	maskPasswordRegex2 = regexp.MustCompile(`[p|P]assword:(\s*)(.*?)(\s*\w+:|$)`)
	maskPasswordRegex3 = regexp.MustCompile(`postgres:(([^:]*\:){1})([^@]*)`)
)

//ThroughputCalc runs on scheduled interval to calculate the throughput per second and sends results to a channel
type ThroughputCalc struct {
	tickInterval time.Duration
	previous     float64
	current      chan float64
	Values       chan float64
	running      bool
	lock         sync.Mutex
}

// NewThroughputCalc returns a throughput calculator based on a duration
func NewThroughputCalc(interval time.Duration) *ThroughputCalc {
	return &ThroughputCalc{tickInterval: interval, current: make(chan float64, 1), Values: make(chan float64, 1)}
}

// GetTickInterval returns the tick interval of the throughput calculator.
func (dt *ThroughputCalc) GetTickInterval() time.Duration {
	return dt.tickInterval
}

// SetCurrent sets the value of the counter
func (dt *ThroughputCalc) SetCurrent(value float64) {
	select {
	case dt.current <- value:
	default:
	}
}

// Start the throughput calculator
func (dt *ThroughputCalc) Start() {
	dt.lock.Lock()
	defer dt.lock.Unlock()
	if !dt.running {
		dt.running = true
		ticker := time.NewTicker(dt.tickInterval)
		go func() {
			for range ticker.C {
				if !dt.running {
					return
				}
				current := <-dt.current
				diff := current - dt.previous
				dt.previous = current
				select {
				case dt.Values <- diff / dt.tickInterval.Seconds():
				default:
				}
			}
		}()
	}
}

// MaskPassword is used to mask sensitive password data before outputing to persistent stream like logs.
func MaskPassword(s string) string {
	s = maskPasswordRegex1.ReplaceAllString(s, maskPasswordReplaceString1)
	s = maskPasswordRegex2.ReplaceAllString(s, maskPasswordReplaceString2)
	return maskPasswordRegex3.ReplaceAllString(s, maskPasswordReplaceString3)
}

// ParseEnv takes a prefix string p and *flag.FlagSet. Each flag
// in the FlagSet is exposed as an upper case environment variable
// prefixed with p. Any flag that was not explicitly set by a user
// is updated to the environment variable, if set.
//
// Note: when run with multiple times with different prefixes on the
// same FlagSet, precedence will get values set with prefix which is
// parsed first.
func ParseEnv(p string, fs *flag.FlagSet) {
	// Build a map of explicitly set flags.
	set := make(map[string]struct{})
	fs.Visit(func(f *flag.Flag) {
		set[f.Name] = struct{}{}
	})

	fs.VisitAll(func(f *flag.Flag) {
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
				// Ignore error since only error that can occur is
				// when using a non-set flag name which cannot happen
				// in this situation.
				_ = fs.Set(f.Name, val)
			}
		}

		// Append the env var to the
		// Flag.Usage field.
		f.Usage = fmt.Sprintf("%s [%s]", f.Usage, envVar)
	})
}
