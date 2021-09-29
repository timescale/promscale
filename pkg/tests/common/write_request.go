// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package common

import (
	ingstr "github.com/timescale/promscale/pkg/pgmodel/ingestor"
	"github.com/timescale/promscale/pkg/prompb"
)

// NewWriteRequestWithTs returns a new *prompb.WriteRequest from the pool and applies ts to it if ts is not nil.
func NewWriteRequestWithTs(ts []prompb.TimeSeries) *prompb.WriteRequest {
	wr := ingstr.NewWriteRequest()
	if ts != nil {
		wr.Timeseries = ts
	}
	return wr
}
