package main

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/config"
	"github.com/timescale/promscale/pkg/bench"
)

func main() {
	config := &bench.BenchConfig{
		TSDBPath:                "/Users/arye/promdata/hk_default_tenant",
		Mint:                    math.MinInt64,
		Maxt:                    math.MaxInt64,
		WriteEndpoint:           "http://localhost:9201/write",
		UseWallClockForDataTime: true,
		RateControl:             true,
		RateMultiplier:          10000.0,
		SeriesMultiplier:        1,
		QueueConfig:             config.DefaultQueueConfig,
	}
	if err := config.Validate(); err != nil {
		fmt.Println(err)
		return
	}
	if err := bench.Run(config); err != nil {
		fmt.Println(err)
		return
	}
}

// update modifies the priority and value of an Item in the queue.
//func (pq *PriorityQueue) update(item *SeriesItem, ts int64) {
//item.ts = ts
//heap.Fix(pq, item.index)
//}
