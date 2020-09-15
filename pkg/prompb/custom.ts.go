package prompb

func (m *WriteRequest) Reset() { *m = WriteRequest{Timeseries: m.Timeseries[:0]} }
func (m *Labels) Reset()       { *m = Labels{Labels: m.Labels[:0]} }
