// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package day

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
)

const (
	dayUnit                 = 'd'
	unknownUnitDErrorPrefix = `time: unknown unit "d"`
)

// Duration acts like a time.Duration with support for "d" unit
// which is used for specifying number of days in duration.
type Duration time.Duration

// UnmarshalText unmarshals strings into DayDuration values while
// handling the day unit. It leans heavily into time.ParseDuration.
func (d *Duration) UnmarshalText(s []byte) error {
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
	*d = Duration(val)
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
func (d Duration) String() string {
	return time.Duration(d).String()
}

// StringToDayDurationHookFunc returns a mapstructure.DecodeHookFunc that
// converts strings to DayDuration.
func StringToDayDurationHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		var d Duration

		if t != reflect.TypeOf(d) {
			return data, nil
		}

		err := d.UnmarshalText([]byte(data.(string)))
		if err != nil {
			return nil, err
		}
		return d, nil
	}
}

// String returns the output in form of days:hours:mins:secs
func String(d Duration) string {
	const day = int64(time.Hour * 24)

	remainder := int64(d)
	days := remainder / day
	remainder = remainder % day
	hours := remainder / int64(time.Hour)
	remainder = remainder % int64(time.Hour)
	mins := remainder / int64(time.Minute)
	remainder = remainder % int64(time.Minute)
	secs := remainder / int64(time.Second)

	display := ""
	if days != 0 {
		display = fmt.Sprintf("%dd", days)
	}
	if hours != 0 {
		display = fmt.Sprintf("%s%dh", display, hours)
	}
	if mins != 0 {
		display = fmt.Sprintf("%s%dm", display, mins)
	}
	if secs != 0 {
		display = fmt.Sprintf("%s%ds", display, secs)
	}
	return display
}
