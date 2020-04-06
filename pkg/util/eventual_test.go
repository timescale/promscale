package util

import (
	"fmt"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"sync"
	"testing"
	"time"
)

func init() {
	log.Init("debug")
}

func TestDefEventualSetLocks(t *testing.T) {
	// test can't read while set is called
	e := defEventual{lock: &sync.RWMutex{}}
	valueToSet := 1

	var errorInRoutine error
	lockIsAcqBySet := sync.WaitGroup{}
	lockIsAcqBySet.Add(1)
	e.whenDone = []func(interface{}){func(res interface{}) {
		// the result should not be different than it was
		// at the time this function was called
		if e.result.(int) != res.(int) {
			errorInRoutine = fmt.Errorf(
				"result was changed after whenDone function was invoked; got %v; expected %v",
				res,
				e.result,
			)
		}
		// let the main routine call get
		lockIsAcqBySet.Done()
		// main routine should wait for lock to be released
		time.Sleep(time.Second)
	}}
	getIsCalledByMain := &sync.WaitGroup{}
	getIsCalledByMain.Add(1)
	go func() {
		// wait for the main routine to call get first
		getIsCalledByMain.Wait()
		e.set(valueToSet)
	}()
	res, done := e.Get()
	if res != nil || done {
		t.Fatalf(
			"expected no result and to not be done; got res: %v, done: %v",
			res,
			done,
		)
	}
	getIsCalledByMain.Done()
	// wait for the set method to have been called and then the
	// functions in e.whenDone releases the main routine
	lockIsAcqBySet.Wait()
	// should block until the set() finishes
	// and the new result is set
	res, done = e.Get()
	if res == nil || !done || res.(int) != valueToSet {
		t.Errorf(
			"expected res: %v and done: %v; got %v and %v",
			valueToSet, true, res, done,
		)
	}
	if errorInRoutine != nil {
		t.Error(errorInRoutine)
	}
}

func TestDefEventualSetPanic(t *testing.T) {
	// test panic on second set
	e := defEventual{lock: &sync.RWMutex{}}
	valueToSet := 1
	e.set(valueToSet)
	res, done := e.Get()
	if !done || res == nil || res.(int) != valueToSet {
		t.Errorf(
			"expected res: %v, done: %v; got %v and %v",
			valueToSet, true, res, done,
		)
	}

	defer func() {
		if r := recover(); r != nil {
			// all is good, code panicked
			return
		}
	}()
	e.set(valueToSet)
	t.Errorf("The code did not panic")
}

func TestEventualFinishes(t *testing.T) {
	whenToComplete := time.Now().Add(3 * time.Second)
	valueToSet := 1
	funcThatFailsForAWhile := func() (interface{}, error) {
		if time.Now().Before(whenToComplete) {
			time.Sleep(time.Second)
			return nil, fmt.Errorf("not yet")
		}
		return valueToSet, nil
	}
	e := NewEventual("id", funcThatFailsForAWhile)

	var done = false
	var res interface{}
	for !done {
		now := time.Now()
		res, done = e.Get()
		if !done {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if now.Before(whenToComplete) {
			t.Fatalf("eventual completed too soon")
			return
		}

		if res.(int) != valueToSet {
			t.Errorf("wrong value was set; expected: %d got: %v", valueToSet, res)
			return
		}
	}
}

func TestEventualAddCallback(t *testing.T) {
	e := defEventual{lock: &sync.RWMutex{}}
	var callback1Called bool
	var valueInCallback1 interface{}
	var valueToSet = 1
	//if eventual is not complete, callback is added to queue
	e.AddCallback(func(i interface{}) {
		valueInCallback1 = i
		callback1Called = true
	})
	if callback1Called {
		t.Error("callback was called before result was set")
	}
	if len(e.whenDone) != 1 {
		t.Error("callback was not added to queue")
	}
	e.set(valueToSet)
	if !callback1Called || valueInCallback1.(int) != valueToSet {
		t.Errorf("expected callback to be called with %d; got isCalled: %v with value %v",
			valueToSet, callback1Called, valueInCallback1,
		)
	}

	//if eventual is complete, callback is called without adding to queue
	var valueInCallback2 interface{}
	var callback2Called bool
	e.AddCallback(func(i interface{}) {
		valueInCallback2 = i
		callback2Called = true
	})
	if !callback2Called || valueInCallback2.(int) != valueToSet {
		t.Errorf("expected callback to be called with %d; got isCalled: %v with value %v",
			valueToSet, callback2Called, valueInCallback1,
		)
	}
	if len(e.whenDone) > 1 {
		t.Error("callback was added to queue")
	}
}

func TestEventualMap(t *testing.T) {
	// test mapper function is applied
	e := defEventual{lock: &sync.RWMutex{}}
	var mapCalledWith interface{}
	valueToSet := 1
	valueToMap := 2
	mappedE := e.Map("", func(x interface{}) interface{} {
		mapCalledWith = x
		return valueToMap
	})
	if _, done := mappedE.Get(); done {
		t.Error("mapped eventual done, before it's original eventual")
	}
	if len(e.whenDone) != 1 {
		t.Error("expected mapped eventual to be added as callback to original")
	}

	e.set(valueToSet)
	if mapCalledWith.(int) != valueToSet {
		t.Errorf("map function called with %v; expected %d", mapCalledWith, valueToSet)
	}
	if res, done := mappedE.Get(); !done || res.(int) != valueToMap {
		t.Errorf(
			"expected mapped eventual to be complete with %d, got: isComplete %v, res %v",
			valueToMap, done, res,
		)
	}
}

func TestNewEventualWith(t *testing.T) {
	numCalls := uint(0)
	numRetries := uint(3)
	backOff := time.Second
	start := time.Now()
	var took time.Duration
	e := NewEventualWith("id", numRetries, backOff, func() (i interface{}, err error) {
		numCalls += 1
		return nil, fmt.Errorf("some error")
	})
	e.AddCallback(func(i interface{}) {
		took = time.Since(start)
	})
	//test if eventual works in background (should not be done)
	if _, done := e.Get(); done {
		t.Errorf("eventual either didn't sleep between backoffs or worked on same routine")
	}
	time.Sleep(time.Duration(numRetries) * backOff)
	if _, done := e.Get(); !done {
		t.Errorf("eventual didn't complete on time")
	}
	if numCalls != numRetries {
		t.Errorf("result function called %d times; expected %d", numCalls, numRetries)
	}
	if took == 0 {
		t.Errorf("callback wasn't called")
	}
	// n retries = (n-1) sleeps in between
	expDuration := time.Duration(numRetries-1) * backOff
	if took < expDuration {
		t.Errorf(
			"eventual completed too fast, callback was called in %s expected at least %s",
			took.String(), expDuration.String(),
		)
	}
}
