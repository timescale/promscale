// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"fmt"
	"strings"
	"time"
)

const (
	dayUnit                 = 'd'
	unknownUnitDErrorPrefix = `time: unknown unit "d"`
)

// DayDuration acts like a time.Duration with support for "d" unit
// which is used for specifying number of days in duration.
type DayDuration time.Duration

// UnmarshalText unmarshals strings into DayDuration values while
// handling the day unit. It leans heavily into time.ParseDuration.
func (d *DayDuration) UnmarshalText(s []byte) error {
	val, err := time.ParseDuration(string(s))
	if err != nil {
		// Check for specific error indicating we are using days unit.
		if !strings.HasPrefix(err.Error(), unknownUnitDErrorPrefix) {
			return err
		}

		val, err = handleDays(s)
		if err != nil {
			return err
		}
	}
	*d = DayDuration(val)
	return nil
}

func handleDays(s []byte) (time.Duration, error) {
	parts := strings.Split(string(s), string(dayUnit))

	if len(parts) > 2 {
		return 0, fmt.Errorf(`time: invalid duration "%s"`, string(s))
	}

	// Treating first part as hours and multiplying with 24 to get duration in days.
	days, err := time.ParseDuration(parts[0] + "h")
	if err != nil {
		return 0, fmt.Errorf(`time: invalid duration "%s"`, string(s))
	}
	days = days * 24

	if s[len(s)-1] == dayUnit {
		return days, nil
	}

	val, err := time.ParseDuration(parts[1])
	if err != nil {
		return 0, fmt.Errorf(`time: invalid duration "%s"`, string(s))
	}

	return val + days, nil
}

// String returns a string value of DayDuration.
func (d DayDuration) String() string {
	return time.Duration(d).String()
}
