package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/felixge/fgprof"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/timescale/promscale/pkg/bench"
)

func main() {
	http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
	go func() {
		log.Println(http.ListenAndServe(":8080", nil))
	}()

	config := &bench.BenchConfig{
		TSDBPath:                "/mnt/tsdb",
		Mint:                    math.MinInt64,
		Maxt:                    math.MaxInt64,
		WriteEndpoint:           "http://localhost:9201/write",
		UseWallClockForDataTime: true,
		RateControl:             true,
		RateMultiplier:          1.0,
		SeriesMultiplier:        1,
		MetricMultiplier:        1,
		RemoteWriteConfig:       config.DefaultRemoteWriteConfig,
		RepeatedRuns:            1,
	}

	//just for imports
	_ = model.Duration(time.Second)

	//config.RemoteWriteConfig.QueueConfig.MinShards = 32
	//config.RemoteWriteConfig.QueueConfig.MaxShards = 32
	//config.RemoteWriteConfig.QueueConfig.Capacity = 300000
	//config.RemoteWriteConfig.QueueConfig.MaxSamplesPerSend = 30000
	//config.RemoteWriteConfig.QueueConfig.BatchSendDeadline = model.Duration(30 * time.Second)

	//config.RemoteWriteConfig.RemoteTimeout = model.Duration(90 * time.Second)

	if err := config.Validate(); err != nil {
		fmt.Println(err)
		return
	}
	if err := bench.Run(config); err != nil {
		fmt.Println(err)
		return
	}
}
