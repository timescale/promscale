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
		return prompb.NewWriteRequest()
	},
}

// NewWriteRequest returns a new *prompb.WriteRequest from the pool.
func NewWriteRequest() *prompb.WriteRequest {
	return wrPool.Get().(*prompb.WriteRequest)
}

// FinishWriteRequest adds the *prompb.WriteRequest back into the pool after setting parameters to default.
func FinishWriteRequest(wr *prompb.WriteRequest) {
	if wr == nil {
		return
	}
	wr.Reset()
	wrPool.Put(wr)
}
