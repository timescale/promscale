package pgmodel

import (
	"sync"

	"github.com/prometheus/prometheus/prompb"
)

var pool = sync.Pool{
	New: func() interface{} {
		return new(InsertCtx)
	},
}

type InsertCtx struct {
	WriteRequest prompb.WriteRequest
}

func NewInsertCtx() *InsertCtx {
	return pool.Get().(*InsertCtx)
}

func (t *InsertCtx) clear() {
	for _, ts := range t.WriteRequest.Timeseries {
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
	}
	t.WriteRequest.Timeseries = t.WriteRequest.Timeseries[:0]
}

func (t *InsertCtx) Close() {
	t.clear()
	pool.Put(t)
}
