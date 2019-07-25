package util

import (
	"fmt"
	"sync"
	"time"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
)

//ThroughputCalc runs on scheduled interval to calculate the throughput per second and sends results to a channel
type ThroughputCalc struct {
	tickInterval time.Duration
	previous     float64
	current      chan float64
	Values       chan float64
	running      bool
	lock         sync.Mutex
}

func NewThroughputCalc(interval time.Duration) *ThroughputCalc {
	return &ThroughputCalc{tickInterval: interval, current: make(chan float64, 1), Values: make(chan float64, 1)}
}

func (dt *ThroughputCalc) SetCurrent(value float64) {
	select {
	case dt.current <- value:
	default:
	}
}

func (dt *ThroughputCalc) Start() {
	dt.lock.Lock()
	defer dt.lock.Unlock()
	if !dt.running {
		dt.running = true
		ticker := time.NewTicker(dt.tickInterval)
		go func() {
			for range ticker.C {
				if !dt.running {
					return
				}
				current := <-dt.current
				diff := current - dt.previous
				dt.previous = current
				select {
				case dt.Values <- diff / dt.tickInterval.Seconds():
				default:
				}
			}
		}()
	}
}

// Blocking retry with a fixed delay
func RetryWithFixedDelay(retries uint, wait time.Duration, f func() (interface{}, error)) (interface{}, error) {
	current := uint(0)
	for {
		value, err := f()
		if err == nil {
			return value, nil
		}
		log.Error("msg", "Error running function with retry", "err", err)
		current++
		if current >= retries {
			log.Error("msg", fmt.Sprintf("Giving up retrying after %d failed attempts", retries))
			return nil, err
		}
		log.Debug("Sleeping for %d(ns) before next retry", wait.Nanoseconds())
		time.Sleep(wait)
	}
}
