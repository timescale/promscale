// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"sync"

	"github.com/timescale/promscale/pkg/prompb"
)

var wrPool = sync.Pool{
	New: func() interface{} {
		return new(prompb.WriteRequest)
	},
}

// NewWriteRequest returns a new *prompb.WriteRequest from the pool.
func NewWriteRequest() *prompb.WriteRequest {
	return wrPool.Get().(*prompb.WriteRequest)
}

// FinishWriteRequest adds the *prompb.WriteRequest back into the pool after setting parameters to default.
func FinishWriteRequest(wr *prompb.WriteRequest) {
	for i := range wr.Timeseries {
		ts := &wr.Timeseries[i]
		for j := range ts.Labels {
			ts.Labels[j] = prompb.Label{}
		}
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
		ts.XXX_unrecognized = nil
	}
	wr.Timeseries = wr.Timeseries[:0]
	wr.Metadata = wr.Metadata[:0]
	wr.XXX_unrecognized = nil
	wrPool.Put(wr)
}
