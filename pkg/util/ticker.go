package util

import "time"

type Ticker interface {
	Channel() <-chan time.Time
	Stop()
}

type ticker struct {
	*time.Ticker
}

func (t *ticker) Channel() <-chan time.Time {
	return t.C
}

func NewTicker(d time.Duration) Ticker {
	return &ticker{time.NewTicker(d)}
}

type ManualTicker struct {
	C chan time.Time
}

func (m *ManualTicker) Channel() <-chan time.Time {
	return m.C
}

func (m *ManualTicker) Tick() {
	m.C <- time.Now()
}
func (m *ManualTicker) Wait() {
	<-m.C
}

func (m *ManualTicker) Stop() {
	panic("not implemented")
}

func NewManualTicker(channelSize int) *ManualTicker {
	return &ManualTicker{C: make(chan time.Time, channelSize)}
}
