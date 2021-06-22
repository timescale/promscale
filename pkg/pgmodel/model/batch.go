//todo: header files.
package model

import (
	"fmt"
	"math"
	"time"

	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/prompb"
)

type InsertableType uint8

const (
	Invalid InsertableType = iota
	Sample
	Exemplar
)

type Insertable interface {
	GetSeries() *Series
	Count() int
	At(index int) sampleFields
	Type() InsertableType
	data() interface{}
	AllExemplarLabelKeys() []string
	OrderExemplarLabels(index map[string]int) (positionExists bool)
}

type sampleFields interface {
	// GetTs returns timestamp of the field (sample or exemplar).
	// It is based on non-pointer declaration to save allocs (see types.pb.go)
	// and also saves from type checks due to interface (as At() will then return interface{}).
	GetTs() int64
	// GetVal similar to GetTs, but returns value.
	GetVal() float64
	// ExemplarLabels returns labels of exemplars.
	ExemplarLabels() []prompb.Label
}

func NewInsertable(series *Series, data interface{}) Insertable {
	if data == nil {
		return newNoopInsertable(series)
	}
	switch n := data.(type) {
	case []prompb.Sample:
		return newPromSamples(series, n)
	case []prompb.Exemplar:
		return newExemplarSamples(series, n)
	default:
		panic("invalid insertableType")
	}
}

// Data wraps incoming data with its in-timestamp. It is used to warn if the rate
// of incoming samples vs outgoing samples is too low, based on time.
type Data struct {
	Rows         map[string][]Insertable
	ReceivedTime time.Time
}

// Batch is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type Batch struct {
	data        []Insertable
	seriesIndex int
	dataIndex   int
	MinSeen     int64
	err         error
}

// NewBatch returns a new batch that can hold samples and exemplars.
func NewBatch() Batch {
	si := Batch{data: make([]Insertable, 0)}
	si.ResetPosition()
	return si
}

func (t *Batch) Reset() {
	for i := 0; i < len(t.data); i++ {
		// nil all pointers to prevent memory leaks
		t.data[i] = nil
	}
	*t = Batch{data: t.data[:0]}
	t.ResetPosition()
}

func (t *Batch) CountSeries() int {
	return len(t.data)
}

func (t *Batch) Data() []Insertable {
	return t.data
}

func (t *Batch) Count() (numSamples, numExemplars int) {
	for i := range t.data {
		switch t.data[i].Type() {
		case Sample:
			numSamples += t.data[i].Count()
		case Exemplar:
			numExemplars += t.data[i].Count()
		default:
			panic(fmt.Sprintf("invalid type %d", t.data[i].Type()))
		}
	}
	return
}

// Append adds a sample info to the back of the iterator
func (t *Batch) Append(s Insertable) {
	t.data = append(t.data, s)
}

func (t *Batch) AppendSlice(s []Insertable) {
	t.data = append(t.data, s...)
}

//ResetPosition resets the iteration position to the beginning
func (t *Batch) ResetPosition() {
	t.dataIndex = -1
	t.seriesIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *Batch) Next() bool {
	t.dataIndex++
	if t.seriesIndex < len(t.data) && t.dataIndex >= t.data[t.seriesIndex].Count() {
		t.seriesIndex++
		t.dataIndex = 0
	}
	return t.seriesIndex < len(t.data)
}

func (t *Batch) dataType() InsertableType {
	return t.data[t.seriesIndex].Type()
}

// Values returns the values for the current row
func (t *Batch) Values() (time.Time, float64, SeriesID, SeriesEpoch, InsertableType) {
	set := t.data[t.seriesIndex]
	sample := set.At(t.dataIndex)
	if t.MinSeen > sample.GetTs() {
		t.MinSeen = sample.GetTs()
	}
	sid, eid, err := set.GetSeries().GetSeriesID()
	if t.err == nil {
		t.err = err
	}
	return model.Time(sample.GetTs()).Time(), sample.GetVal(), sid, eid, set.Type()
}

// GetCorrespondingLabelValues returns the label values of the underlying (current) data in the iterator (i.e., exemplar).
func (t *Batch) GetCorrespondingLabelValues() []string {
	set := t.data[t.seriesIndex]
	exemplar := set.At(t.dataIndex)
	lbls := exemplar.ExemplarLabels()
	s := make([]string, len(lbls))
	for i := range lbls {
		s[i] = lbls[i].Value
	}
	return s
}

func (t *Batch) Absorb(other Batch) {
	t.AppendSlice(other.data)
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *Batch) Err() error {
	return t.err
}

// Release puts the underlying promSamples back into its pool so that they can be reused.
func (t *Batch) Release() {
	for i := range t.data {
		data := t.data[i].data()
		switch n := data.(type) {
		case *promSamples:
			putSamples(n)
		case *promExemplars:
			putExemplars(n)
		}
	}
}

type noopInsertable struct {
	series *Series
}

func newNoopInsertable(s *Series) Insertable {
	return &noopInsertable{
		series: s,
	}
}

func (t *noopInsertable) GetSeries() *Series {
	return t.series
}

func (t *noopInsertable) Count() int {
	return 0
}

func (t *noopInsertable) At(_ int) sampleFields {
	return nil
}

func (t *noopInsertable) Type() InsertableType {
	return Invalid
}

func (t *noopInsertable) AllExemplarLabelKeys() []string {
	return nil
}

func (t *noopInsertable) OrderExemplarLabels(_ map[string]int) bool { return false }

func (t *noopInsertable) data() interface{} {
	return nil
}
