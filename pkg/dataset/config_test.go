package dataset

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		cfg   Config
		err   string
	}{
		{
			name:  "invalid config yaml",
			input: "invalid",
			err:   "yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `invalid` into dataset.Config",
		},
		{
			name:  "happy path",
			input: "metrics:\n  default_chunk_interval: 3h",
			cfg:   Config{Metrics: Metrics{ChunkInterval: 3 * time.Hour}},
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {

			cfg, err := NewConfig(c.input)

			if c.err != "" {
				require.EqualError(t, err, c.err)
				return
			}

			require.Equal(t, cfg, c.cfg)
		})
	}
}
