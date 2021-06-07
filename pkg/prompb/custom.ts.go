package prompb

func (m *Labels) Reset() { *m = Labels{Labels: m.Labels[:0]} }
func (m *WriteRequest) Reset() {
	*m = WriteRequest{Timeseries: m.Timeseries[:0], Metadata: m.Metadata[:0]}
}
func (m *TimeSeries) Reset() {
	*m = TimeSeries{Labels: m.Labels[:0], Exemplars: m.Exemplars[:0], Samples: m.Samples[:0]}
}
func (m *Exemplar) Reset() { *m = Exemplar{Labels: m.Labels[:0]} }
