package api

import (
	"testing"
	"time"
)

func TestCreateMetrics(t *testing.T) {
	testCases := []struct {
		name             string
		tickInterval     int
		expectedInterval time.Duration
	}{
		{
			name:             "tick interval less than default",
			tickInterval:     0,
			expectedInterval: defaultTickInterval,
		},
		{
			name:             "tick interval more than default",
			tickInterval:     5,
			expectedInterval: 5 * time.Second,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			metrics := createMetrics(c.tickInterval)

			if metrics.WriteThroughput.GetTickInterval() != c.expectedInterval {
				t.Errorf("wrong write throughput tick interval, got %s, wanted %s",
					metrics.WriteThroughput.GetTickInterval().String(),
					c.expectedInterval.String())
			}

		})
	}
}
