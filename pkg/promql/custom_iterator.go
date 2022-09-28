package promql

import (
	"fmt"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type samplesIterator interface {
	Reset(it chunkenc.Iterator)
	PeekPrev() (t int64, v float64, ok bool)
	Seek(t int64) bool
	Next() bool
	At() (int64, float64)
	Err() error
}

type rollupItr struct {
	it        chunkenc.Iterator
	prevValue float64
	prevTime  int64
}

func newRollupIterator() *rollupItr {
	fmt.Println("using rollup iterator")
	return &rollupItr{}
}

func (r *rollupItr) Reset(it chunkenc.Iterator) {
	r.it = it
}

func (r *rollupItr) PeekPrev() (t int64, v float64, ok bool) {
	if r.prevTime == 0 {
		return 0, 0, false
	}
	return r.prevTime, r.prevValue, true
}

func (r *rollupItr) Next() bool {
	return r.it.Next()
}

func (r *rollupItr) At() (int64, float64) {
	return r.it.At()
}

func (r *rollupItr) Seek(t int64) bool {
	for r.Next() {
		ts, v := r.At()
		r.prevTime, r.prevValue = ts, v
		if ts >= t {
			return true
		}
	}
	return false
}

func (r *rollupItr) Err() error {
	return r.it.Err()
}
