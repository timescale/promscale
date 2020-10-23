// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"regexp"
	"sync"
	"time"
)

const (
	PromNamespace              = "ts_prom"
	maskPasswordReplaceString1 = "password=$1'****'"
	maskPasswordReplaceString2 = "password:$1****$3"
)

var (
	maskPasswordRegex1 = regexp.MustCompile(`[p|P]assword=(\s*?)'([^']+?)'`)
	maskPasswordRegex2 = regexp.MustCompile(`[p|P]assword:(\s*?)([^\s]+?)( |$)`)
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

// NewThroughputCalc returns a throughput calculator based on a duration
func NewThroughputCalc(interval time.Duration) *ThroughputCalc {
	return &ThroughputCalc{tickInterval: interval, current: make(chan float64, 1), Values: make(chan float64, 1)}
}

// SetCurrent sets the value of the counter
func (dt *ThroughputCalc) SetCurrent(value float64) {
	select {
	case dt.current <- value:
	default:
	}
}

// Start the throughput calculator
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

// MaskPassword is used to mask sensitive password data before outputing to persistent stream like logs.
func MaskPassword(s string) string {
	s = maskPasswordRegex1.ReplaceAllString(s, maskPasswordReplaceString1)
	return maskPasswordRegex2.ReplaceAllString(s, maskPasswordReplaceString2)
}
