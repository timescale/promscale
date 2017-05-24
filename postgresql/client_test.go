package pgprometheus

import (
	"testing"

	"github.com/prometheus/prometheus/storage/remote"
)

func TestBuildCommand(t *testing.T) {
	c := &Client{
		cfg: &Config{
			pgPrometheusNormalizedTable: "metrics",
			pgPrometheusNormalize:       true,
		},
	}

	q := &remote.Query{
		StartTimestampMs: 0,
		EndTimestampMs:   20000,
		Matchers: []*remote.LabelMatcher{
			&remote.LabelMatcher{
				Type:  remote.MatchType_EQUAL,
				Name:  "__name__",
				Value: "cpu_usage",
			},
			&remote.LabelMatcher{
				Type:  remote.MatchType_EQUAL,
				Name:  "job",
				Value: "nginx",
			},
			&remote.LabelMatcher{
				Type:  remote.MatchType_REGEX_MATCH,
				Name:  "host",
				Value: "local.*",
			},
		},
	}

	cmd, err := c.buildCommand(q)

	if err != nil {
		t.Fatal(err)
	}

	t.Log(cmd)
}
