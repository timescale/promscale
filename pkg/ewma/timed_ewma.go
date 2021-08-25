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

	alpha         float64
	lastEvents    int64
	lastCount     int64
	lastTimedRate float64
	lastWallRate  float64
	lastWallTime  time.Time
	init          bool
	mutex         sync.Mutex
}

// NewEWMARate always allocates a new ewmaRate, as this guarantees the atomically
// accessed int64 will be aligned on ARM.  See prometheus#2666.
func NewTimedEWMARate(alpha float64) *TimedRate {
	return &TimedRate{
		alpha: alpha,
	}
}

func (r *TimedRate) Events() int64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastEvents
}

func (r *TimedRate) Count() int64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastCount
}

func (r *TimedRate) WallRate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastWallRate
}

// Rate returns the per-second rate.
func (r *TimedRate) TimedRate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastTimedRate
}

// Tick assumes to be called every r.interval.
func (r *TimedRate) Tick() {
	newEvents := atomic.SwapInt64(&r.newEvents, 0)
	newDuration := atomic.SwapInt64(&r.newDuration, 0)
	newCount := atomic.SwapInt64(&r.newCount, 0)

	instantRateTimed := float64(newEvents) / time.Duration(newDuration).Seconds()
	instantRateWall := float64(newEvents) / time.Since(r.lastWallTime).Seconds()

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.init {
		r.lastTimedRate += r.alpha * (instantRateTimed - r.lastTimedRate)
		r.lastWallRate += r.alpha * (instantRateWall - r.lastWallRate)
	} else if newEvents > 0 {
		r.init = true
		r.lastTimedRate = instantRateTimed
		r.lastWallRate = 0
	}
	r.lastEvents = newEvents
	r.lastCount = newCount
	r.lastWallTime = time.Now()
}

// Incr counts incr events.
func (r *TimedRate) Incr(events int64, took time.Duration) {
	atomic.AddInt64(&r.newEvents, events)
	atomic.AddInt64(&r.newDuration, int64(took))
	atomic.AddInt64(&r.newCount, int64(1))
}
