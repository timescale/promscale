package bench

import (
	"fmt"

	"github.com/prometheus/prometheus/config"
)

type BenchConfig struct {
	TSDBPath                string
	Mint                    int64
	Maxt                    int64
	QueueConfig             config.QueueConfig
	WriteEndpoint           string
	UseWallClockForDataTime bool

	// Be careful if turning off rate control as turning it off will mess up
	// The rate used as "input" for dynamic shard number adjustment
	RateControl      bool
	RateMultiplier   float64
	SeriesMultiplier int
}

func (t *BenchConfig) Validate() error {
	if !t.RateControl {
		fmt.Println("Warning, rate control is off -- dynamic resharding will not work")
	}
	return nil
}
