// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package prompb

func (m *Labels) Reset() { *m = Labels{Labels: m.Labels[:0]} }
func (m *WriteRequest) Reset() {
	*m = WriteRequest{Timeseries: m.Timeseries[:0], Metadata: m.Metadata[:0]}
}
func (m *TimeSeries) Reset() {
	*m = TimeSeries{Labels: m.Labels[:0], Exemplars: m.Exemplars[:0], Samples: m.Samples[:0]}
}
func (m *Exemplar) Reset() { *m = Exemplar{Labels: m.Labels[:0]} }
