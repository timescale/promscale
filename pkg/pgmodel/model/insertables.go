// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"fmt"

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
		panic(fmt.Sprintf("invalid insertableType: %T", data))
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
