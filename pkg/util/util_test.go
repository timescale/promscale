// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"flag"
	"os"
	"reflect"
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
		"password='foobar' host='localhost'":                                 "password='****' host='localhost'",
		"password='foo bar' host='localhost'":                                "password='****' host='localhost'",
		"Password='foo bar' host='localhost'":                                "password='****' host='localhost'",
		"password='foo'bar' host='localhost'":                                "password='****'bar' host='localhost'",
		"password= 'foobar' host='localhost'":                                "password= '****' host='localhost'",
		"password=  'foobar' host='localhost'":                               "password=  '****' host='localhost'",
		"pass='foobar' host='localhost'":                                     "pass='foobar' host='localhost'",
		"host='localhost' password='foobar'":                                 "host='localhost' password='****'",
		"password:foobar  host: localhost":                                   "password:****  host: localhost",
		"password:foo bar  host: localhost":                                  "password:**** bar  host: localhost",
		"password: foobar  host: localhost":                                  "password: ****  host: localhost",
		"password:  foobar  host: localhost":                                 "password:  ****  host: localhost",
		"pass:foobar host: localhost":                                        "pass:foobar host: localhost",
		"host: localhost password: foobar":                                   "host: localhost password: ****",
		"host: localhost Password: foobar":                                   "host: localhost password: ****",
		"postgres://postgres:password@localhost:5432/postgres?sslmode=allow": "postgres://postgres:****@localhost:5432/postgres?sslmode=allow",
		"&{ListenAddr::9201 PgmodelCfg:{Host:localhost Port:5432 User:postgres password: password SslMode:require DbConnectRetries:0 AsyncAcks:false ReportInterval:0 LabelsCacheSize:10000 MetricsCacheSize:10000 SeriesCacheSize:0 WriteConnectionsPerProc:4 MaxConnections:-1 UsesHA:false DbUri:postgres://postgres:password@localhost:5432/postgres?sslmode=allow} LogCfg:{Level:debug Format:logfmt} APICfg:{AllowedOrigin:^(?:.*)$ ReadOnly:false AdminAPIEnabled:false TelemetryPath:/metrics Auth:0xc0000a6690} TLSCertFile: TLSKeyFile: HaGroupLockID:0 PrometheusTimeout:-1ns ElectionInterval:5s Migrate:true StopAfterMigrate:false UseVersionLease:true InstallTimescaleDB:true}": "&{ListenAddr::9201 PgmodelCfg:{Host:localhost Port:5432 User:postgres password: **** SslMode:require DbConnectRetries:0 AsyncAcks:false ReportInterval:0 LabelsCacheSize:10000 MetricsCacheSize:10000 SeriesCacheSize:0 WriteConnectionsPerProc:4 MaxConnections:-1 UsesHA:false DbUri:postgres://postgres:****@localhost:5432/postgres?sslmode=allow} LogCfg:{Level:debug Format:logfmt} APICfg:{AllowedOrigin:^(?:.*)$ ReadOnly:false AdminAPIEnabled:false TelemetryPath:/metrics Auth:0xc0000a6690} TLSCertFile: TLSKeyFile: HaGroupLockID:0 PrometheusTimeout:-1ns ElectionInterval:5s Migrate:true StopAfterMigrate:false UseVersionLease:true InstallTimescaleDB:true}",
		"&{ListenAddr::9201 PgmodelCfg:{Host:localhost Port:5432 User:postgres password: pass SslMode:require DbConnectRetries:0 AsyncAcks:false ReportInterval:0 LabelsCacheSize:10000 MetricsCacheSize:10000 SeriesCacheSize:0 WriteConnectionsPerProc:4 MaxConnections:-1 UsesHA:false DbUri:} LogCfg:{Level:debug Format:logfmt} APICfg:{AllowedOrigin:^(?:.*)$ ReadOnly:false AdminAPIEnabled:false TelemetryPath:/metrics Auth:0xc0000a6690} TLSCertFile: TLSKeyFile: HaGroupLockID:0 PrometheusTimeout:-1ns ElectionInterval:5s Migrate:true StopAfterMigrate:false UseVersionLease:true InstallTimescaleDB:true}":                                                                       "&{ListenAddr::9201 PgmodelCfg:{Host:localhost Port:5432 User:postgres password: **** SslMode:require DbConnectRetries:0 AsyncAcks:false ReportInterval:0 LabelsCacheSize:10000 MetricsCacheSize:10000 SeriesCacheSize:0 WriteConnectionsPerProc:4 MaxConnections:-1 UsesHA:false DbUri:} LogCfg:{Level:debug Format:logfmt} APICfg:{AllowedOrigin:^(?:.*)$ ReadOnly:false AdminAPIEnabled:false TelemetryPath:/metrics Auth:0xc0000a6690} TLSCertFile: TLSKeyFile: HaGroupLockID:0 PrometheusTimeout:-1ns ElectionInterval:5s Migrate:true StopAfterMigrate:false UseVersionLease:true InstallTimescaleDB:true}",
	}

	for input, expected := range testData {
		if output := MaskPassword(input); expected != output {
			t.Errorf("Unexpected function output:\ngot %s\nwanted %s\n", output, expected)
		}
	}
}

type flagValues struct {
	First  string
	Second string
	Third  string
}

func TestParseEnv(t *testing.T) {
	testCases := []struct {
		name       string
		prefixes   []string
		env        map[string]string
		args       []string
		flagValues flagValues
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

			for _, prefix := range c.prefixes {
				ParseEnv(prefix, fs)
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
