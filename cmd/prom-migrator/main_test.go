package main

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/prometheus/prometheus/util/testutil"
)

func TestParseFlags(t *testing.T) {
	cases := []struct {
		name            string
		input           []string
		expectedConf    *config
		failsValidation bool
		errMessage      string
	}{
		{
			name:  "pass_normal",
			input: []string{"-mint=1000", "-maxt=1001", "-read-url=http://localhost:9090/api/v1/read", "-write-url=http://localhost:9201/write", "-progress-enabled=false"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               1000,
				maxt:               1001,
				readURL:            "http://localhost:9090/api/v1/read",
				writeURL:           "http://localhost:9201/write",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    false,
			},
			failsValidation: false,
		},
		{
			name:  "fail_all_default",
			input: []string{""},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               -1,
				maxt:               -1,
				readURL:            "",
				writeURL:           "",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `remote read storage url and remote write storage url must be specified. Without these, data migration cannot begin`,
		},
		{
			name:  "fail_all_default_space",
			input: []string{"-read-url=  ", "-write-url= "},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               -1,
				maxt:               -1,
				readURL:            "  ",
				writeURL:           " ",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `remote read storage url and remote write storage url must be specified. Without these, data migration cannot begin`,
		},
		{
			name:  "fail_empty_read_url",
			input: []string{"-write-url=http://localhost:9201/write"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               -1,
				maxt:               -1,
				readURL:            "",
				writeURL:           "http://localhost:9201/write",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `remote read storage url needs to be specified. Without read storage url, data migration cannot begin`,
		},
		{
			name:  "fail_empty_write_url",
			input: []string{"-read-url=http://localhost:9090/api/v1/read"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               -1,
				maxt:               -1,
				readURL:            "http://localhost:9090/api/v1/read",
				writeURL:           "",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `remote write storage url needs to be specified. Without write storage url, data migration cannot begin`,
		},
		{
			name:  "fail_mint_greater_than_maxt",
			input: []string{"-mint=1000000000001", "-maxt=1000000000000", "-read-url=http://localhost:9090/api/v1/read", "-write-url=http://localhost:9201/write"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               1000000000001,
				maxt:               1000000000000,
				readURL:            "http://localhost:9090/api/v1/read",
				writeURL:           "http://localhost:9201/write",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)`,
		},
		{
			name:  "fail_progress_enabled_but_no_read_write_storage_url_provided",
			input: []string{"-mint=1000000000001", "-maxt=1000000000000", "-read-url=http://localhost:9090/api/v1/read", "-write-url=http://localhost:9201/write"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               1000000000001,
				maxt:               1000000000000,
				readURL:            "http://localhost:9090/api/v1/read",
				writeURL:           "http://localhost:9201/write",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "",
				progressEnabled:    true,
			},
			failsValidation: true,
			errMessage:      `invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)`,
		},
		{
			name:  "pass_progress_enabled_and_read_write_storage_url_provided",
			input: []string{"-mint=100000000000", "-maxt=1000000000000", "-read-url=http://localhost:9090/api/v1/read", "-write-url=http://localhost:9201/write", "-writer-read-url=http://localhost:9201/read"},
			expectedConf: &config{
				name:               "prom-migrator",
				mint:               100000000000,
				maxt:               1000000000000,
				readURL:            "http://localhost:9090/api/v1/read",
				writeURL:           "http://localhost:9201/write",
				progressMetricName: "prom_migrator_progress",
				writerReadURL:      "http://localhost:9201/read",
				progressEnabled:    true,
			},
			failsValidation: false,
			errMessage:      `invalid input: minimum timestamp value (mint) cannot be greater than the maximum timestamp value (maxt)`,
		},
	}

	for _, c := range cases {
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		config := new(config)
		parseFlags(config, c.input)
		testutil.Equals(t, c.expectedConf, config, fmt.Sprintf("parse-flags: %s", c.name))
		err := validateConf(config)
		if c.failsValidation {
			if err == nil {
				t.Errorf(fmt.Sprintf("%s should have failed", c.name))
			}
			testutil.Equals(t, c.errMessage, err.Error(), fmt.Sprintf("validation: %s", c.name))
		}
		if err != nil && !c.failsValidation {
			testutil.Ok(t, err)
		}
	}
}
