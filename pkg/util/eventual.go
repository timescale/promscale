package util

import (
	"fmt"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"sync"
	"time"
)

const (
	defaultRetries = 10
	defaultBackOff = time.Second
)

// EventualResult is a function type that returns a
// result that is eventually computed, or an error
// that happened during the calculation.
// To be used when creating Eventual objects
type EventualResult func() (interface{}, error)

// CallbackOnEventualResult is a function type
// that is called when an EventualObject has completed.
// The input argument is the calculated result or
// nil if an error occurred.
type CallbackOnEventualResult func(interface{})

// Eventual defines an interface for a result
// that eventually finishes being calculated, and when done
// it calls all registered callbacks with the result
type Eventual interface {
	// Get (calculated result, isDone) or (nil, false)
	// if the result is not ready yet
	// A (nil, true) result should be considered
	// as an error occurred
	Get() (interface{}, bool)
	// AddCallback to be called after the result
	// is calculated but before Get() returns true
	// if the result is already calculated, the callback
	// is immediately called
	AddCallback(CallbackOnEventualResult)
	// Map creates a new Eventual that completes
	// at the same time as this, but the result is
	// transformed/mapped using the provided function
	Map(string, func(interface{}) interface{}) Eventual
}

// NewEventual creates an Eventual implementation with the
// supplied id, The result from supplied function fn is stored
// in the Eventual object, if fn returns an error, the call is retried
// after a default back off period (1 second). The call is retried
// for a default number of retries (10).
func NewEventual(id string, fn func() (interface{}, error)) Eventual {
	return NewEventualWith(id, defaultRetries, defaultBackOff, fn)
}

// NewEventualWith creates an Eventual implementation with the
// supplied id, The result from supplied function fn is stored
// in the Eventual object, if fn returns an error, the call is retried
// after the supplied back off period. The call is retried
// numRetries number of times.
func NewEventualWith(id string, numRetries uint, backOff time.Duration, fn EventualResult) Eventual {
	e := &defEventual{id: id, lock: &sync.RWMutex{}}
	go e.retry(numRetries, backOff, fn)
	return e
}

type defEventual struct {
	id       string
	lock     *sync.RWMutex
	result   interface{}
	isDone   bool
	whenDone []func(interface{})
}

func (e *defEventual) Map(id string, mapFn func(interface{}) interface{}) Eventual {
	newEventual := &defEventual{id: id, lock: &sync.RWMutex{}}
	e.AddCallback(func(res interface{}) {
		mappedRes := mapFn(res)
		newEventual.set(mappedRes)
	})
	return newEventual
}

func (e *defEventual) AddCallback(f CallbackOnEventualResult) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if !e.isDone {
		e.whenDone = append(e.whenDone, f)
	} else {
		f(e.result)
	}
}

func (e *defEventual) Get() (interface{}, bool) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.result, e.isDone
}

func (e *defEventual) set(res interface{}) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.isDone {
		panic("calculation in Eventual object was already done, can't set another value")
	}
	e.isDone = true
	e.result = res
	log.Debug("msg", fmt.Sprintf("%s completed, calling callbacks", e.id))
	for _, f := range e.whenDone {
		f(e.result)
	}
}

func (e *defEventual) retry(attempts uint, sleep time.Duration, f func() (interface{}, error)) {
	var err error
	var res interface{}
	for i := uint(0); ; i++ {
		res, err = f()
		if err == nil {
			e.set(res)
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)
		log.Debug("msg", fmt.Errorf("%s will retry; error received: %s", e.id, err))
	}
	log.Info("msg", "%s failed after %d attempts; last error: %s", e.id, attempts, err)
	e.set(nil)
}
