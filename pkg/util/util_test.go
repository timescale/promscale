package util

import (
	"fmt"
	"testing"
	"time"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
)

const (
	hundredMs = time.Millisecond * 100
)

func init() {
	log.Init("debug")
}

func TestRetryWithoutError(t *testing.T) {
	dummy := func() (interface{}, error) {
		return "Success!", nil
	}

	result, err := RetryWithFixedDelay(1, hundredMs, dummy)
	if err != nil {
		t.Error("Should not return error!", err)
	}
	if result.(string) != "Success!" {
		t.Error("Expecting Success but received: ", result)
	}
}

func TestRetryWithError(t *testing.T) {
	counter := 0
	retries := uint(2)
	fail := func() (interface{}, error) {
		counter++
		return nil, fmt.Errorf("failed")
	}
	result, err := RetryWithFixedDelay(retries, hundredMs, fail)
	if err == nil {
		t.Error("Should fail after retrying!")
	}
	if counter != 2 {
		t.Errorf("Expected %d invocations but got %d", retries, counter)
	}
	if result != nil {
		t.Error("Should not receive result, but got ", result)
	}
}

func TestSuccessAfterRetry(t *testing.T) {
	counter := 0
	dummy := func() (interface{}, error) {
		counter++
		if counter%2 == 1 {
			return nil, fmt.Errorf("failed")
		}
		return "Success!", nil
	}
	result, err := RetryWithFixedDelay(3, hundredMs, dummy)
	if err != nil {
		t.Error("Should not return error!", err)
	}
	if result.(string) != "Success!" {
		t.Error("Expecting Success but received: ", result)
	}
	if counter != 2 {
		t.Error("Wrong invocation counter ", counter)
	}

}
