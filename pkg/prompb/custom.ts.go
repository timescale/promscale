package prompb

func (m *Labels) Reset() { *m = Labels{Labels: m.Labels[:0]} }
func (m *WriteRequest) Reset() {
	*m = WriteRequest{Timeseries: m.Timeseries[:0], Metadata: m.Metadata[:0]}
}
func (m *TimeSeries) Reset() {
	*m = TimeSeries{Labels: m.Labels[:0], Samples: m.Samples[:0]}
}
