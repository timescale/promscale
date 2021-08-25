// This code has been borrowed from https://github.com/prometheus/prometheus/blob/main/storage/remote/ewma.go
//
// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ewma

import (
	"sync"
	"sync/atomic"
	"time"
)

// Rate tracks an exponentially weighted moving average of a per-second rate with explicit durations.
type TimedRate struct {
	newEvents   int64
	newDuration int64
	newCount    int64

	alpha    float64
	lastRate float64
	init     bool
	mutex    sync.Mutex
}

// NewEWMARate always allocates a new ewmaRate, as this guarantees the atomically
// accessed int64 will be aligned on ARM.  See prometheus#2666.
func NewTimedEWMARate(alpha float64) *TimedRate {
	return &TimedRate{
		alpha: alpha,
	}
}

// Rate returns the per-second rate.
func (r *TimedRate) Rate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastRate
}

// Tick assumes to be called every r.interval.
func (r *TimedRate) Tick() {
	newEvents := atomic.SwapInt64(&r.newEvents, 0)
	newDurationSum := atomic.SwapInt64(&r.newDuration, 0)
	newCount := atomic.SwapInt64(&r.newCount, 0)
	newDurationAvg := newDurationSum / newCount
	instantRate := float64(newEvents) / time.Duration(newDurationAvg).Seconds()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.init {
		r.lastRate += r.alpha * (instantRate - r.lastRate)
	} else if newEvents > 0 {
		r.init = true
		r.lastRate = instantRate
	}
}

// Incr counts incr events.
func (r *TimedRate) Incr(events int64, took time.Duration) {
	atomic.AddInt64(&r.newEvents, events)
	atomic.AddInt64(&r.newDuration, int64(took))
	atomic.AddInt64(&r.newCount, int64(1))
}
