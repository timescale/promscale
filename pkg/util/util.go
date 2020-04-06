// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package util

import (
	"sync"
	"time"
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
