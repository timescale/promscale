package bench

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/config"
)

type BenchConfig struct {
	TSDBPath                string
	Mint                    int64
	Maxt                    int64
	RemoteWriteConfig       config.RemoteWriteConfig
	WriteEndpoint           string
	UseWallClockForDataTime bool

	Concurrency int

	// Be careful if turning off rate control as turning it off will mess up
	// The rate used as "input" for dynamic shard number adjustment
	RateControl      bool
	RateMultiplier   float64
	SeriesMultiplier int
	MetricMultiplier int
	RepeatedRuns     int
	FakeSendDuration time.Duration
}

func (t *BenchConfig) Validate() error {
	if !t.RateControl && t.RemoteWriteConfig.QueueConfig.MinShards != t.RemoteWriteConfig.QueueConfig.MaxShards {
		return fmt.Errorf("Rate control is off -- dynamic resharding will not work")
	}
	if !t.UseWallClockForDataTime && t.RepeatedRuns > 1 {
		return fmt.Errorf("Cannot have repeated runs without using wall clock time. Every run will override data")
	}
	return nil
}
