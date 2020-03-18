package pgmodel

import (
	"encoding/binary"
	"fmt"

	"github.com/allegro/bigcache"
)

var (
	// ErrEntryNotFound is returned when entry is not found.
	ErrEntryNotFound = fmt.Errorf("entry not found")
)

type bCache struct {
	series *bigcache.BigCache
}

func (b *bCache) GetSeries(lset Labels) (SeriesID, error) {
	result, err := b.series.Get(lset.String())
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return SeriesID(binary.LittleEndian.Uint64(result)), nil
}

func (b *bCache) SetSeries(lset Labels, id SeriesID) error {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, uint64(id))
	return b.series.Set(lset.String(), byteID)
}

func uint64Bytes(i uint64) []byte {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, i)
	return byteID
}
