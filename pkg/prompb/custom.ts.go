// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package prompb

func (m *Labels) Reset() { *m = Labels{Labels: m.Labels[:0]} }
func (m *WriteRequest) Reset() {
	for i := range m.Timeseries {
		ts := &m.Timeseries[i]
		for j := range ts.Labels {
			ts.Labels[j] = Label{}
		}
		ts.Labels = ts.Labels[:0]
		ts.Samples = ts.Samples[:0]
		ts.Exemplars = ts.Exemplars[:0]
		ts.XXX_unrecognized = nil
	}
	m.Timeseries = m.Timeseries[:0]
	m.Metadata = m.Metadata[:0]
	m.XXX_unrecognized = nil
	m.XXX_arrayPool.labelsPool = m.XXX_arrayPool.labelsPool[:0]
	m.XXX_arrayPool.samplesPool = m.XXX_arrayPool.samplesPool[:0]
	m.XXX_arrayPool.exemplarsPool = m.XXX_arrayPool.exemplarsPool[:0]
}
func (m *TimeSeries) Reset() {
	*m = TimeSeries{Labels: m.Labels[:0], Exemplars: m.Exemplars[:0], Samples: m.Samples[:0]}
}
func (m *Exemplar) Reset() { *m = Exemplar{Labels: m.Labels[:0]} }
