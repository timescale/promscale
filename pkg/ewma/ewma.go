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

// Rate tracks an exponentially weighted moving average of a per-second rate.
type Rate struct {
	newEvents int64

	alpha    float64
	interval time.Duration
	lastRate float64
	init     bool
	mutex    sync.Mutex
}

// NewEWMARate always allocates a new ewmaRate, as this guarantees the atomically
// accessed int64 will be aligned on ARM.  See prometheus#2666.
func NewEWMARate(alpha float64, interval time.Duration) *Rate {
	return &Rate{
		alpha:    alpha,
		interval: interval,
	}
}

// Rate returns the per-second rate.
func (r *Rate) Rate() float64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.lastRate
}

// Tick assumes to be called every r.interval.
func (r *Rate) Tick() {
	newEvents := atomic.SwapInt64(&r.newEvents, 0)
	instantRate := float64(newEvents) / r.interval.Seconds()

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
func (r *Rate) Incr(incr int64) {
	atomic.AddInt64(&r.newEvents, incr)
}
