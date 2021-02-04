package util

import "time"

type Ticker interface {
	Wait()
	Stop()
}

type ticker struct {
	*time.Ticker
}

func (t *ticker) Wait() { <-t.C }

func NewTicker(d time.Duration) Ticker {
	return &ticker{time.NewTicker(d)}
}
