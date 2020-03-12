package pgmodel

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/allegro/bigcache"
	"github.com/prometheus/prometheus/pkg/labels"
)

var (
	// ErrEntryNotFound is returned when entry is not found.
	ErrEntryNotFound = fmt.Errorf("entry not found")
)

type bCache struct {
	series *bigcache.BigCache
}

func (b *bCache) GetSeries(lset labels.Labels) (SeriesID, error) {
	result, err := b.series.Get(lset.String())
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return SeriesID(binary.LittleEndian.Uint64(result)), nil
}

func (b *bCache) SetSeries(lset labels.Labels, id SeriesID) error {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, uint64(id))
	if !sort.IsSorted(lset) {
		panic("detected a unsorted labels.Labels")
	}
	return b.series.Set(lset.String(), byteID)
}

func uint64Bytes(i uint64) []byte {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, i)
	return byteID
}
