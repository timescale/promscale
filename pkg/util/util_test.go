// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package util

import (
	"testing"
	"time"

	"github.com/timescale/promscale/pkg/log"
)

const (
	step      = 3.0
	frequency = 5.0
	count     = 5.0
)

func init() {
	err := log.Init(log.Config{
		Level: "debug",
	})
	if err != nil {
		panic(err)
	}
}

func TestThroughputCalc(t *testing.T) {

	calc := NewThroughputCalc(time.Second / frequency)
	ticker := time.NewTicker(time.Second / frequency)
	stop := time.NewTimer(time.Second / 2)
	factor := 1.0
	var value float64
	current := step * factor

	calc.Start()

	for range ticker.C {
		calc.SetCurrent(current)
		value = <-calc.Values
		current = current + (step * factor)
		select {
		case <-stop.C:
			if value != step*frequency*factor {
				t.Errorf("Value is not %f, its %f", step*frequency*factor, value)
			}

			factor++

			if factor > count {
				return
			}

			stop.Reset(time.Second / 2)
		default:
		}
	}
}

func TestMaskPassword(t *testing.T) {
	testData := map[string]string{
		"password='foobar' host='localhost'":   "password='****' host='localhost'",
		"password='foo bar' host='localhost'":  "password='****' host='localhost'",
		"Password='foo bar' host='localhost'":  "password='****' host='localhost'",
		"password='foo'bar' host='localhost'":  "password='****'bar' host='localhost'",
		"password= 'foobar' host='localhost'":  "password= '****' host='localhost'",
		"password=  'foobar' host='localhost'": "password=  '****' host='localhost'",
		"pass='foobar' host='localhost'":       "pass='foobar' host='localhost'",
		"host='localhost' password='foobar'":   "host='localhost' password='****'",
		"password:foobar  host: localhost":     "password:****  host: localhost",
		"password:foo bar  host: localhost":    "password:**** bar  host: localhost",
		"password: foobar  host: localhost":    "password: ****  host: localhost",
		"password:  foobar  host: localhost":   "password:  ****  host: localhost",
		"pass:foobar host: localhost":          "pass:foobar host: localhost",
		"host: localhost password: foobar":     "host: localhost password: ****",
		"host: localhost Password: foobar":     "host: localhost password: ****",
	}

	for input, expected := range testData {
		if output := MaskPassword(input); expected != output {
			t.Errorf("Unexpected function output:\ngot %s\nwanted %s\n", output, expected)
		}
	}
}
