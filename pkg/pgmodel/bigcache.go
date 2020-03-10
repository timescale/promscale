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

func (b *bCache) GetSeries(fingerprint uint64) (SeriesID, error) {
	result, err := b.series.Get(uint64String(fingerprint))
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return SeriesID(binary.LittleEndian.Uint64(result)), nil
}

func (b *bCache) SetSeries(fingerprint uint64, id SeriesID) error {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, uint64(id))
	return b.series.Set(uint64String(fingerprint), byteID)
}

func uint64Bytes(i uint64) []byte {
	byteID := make([]byte, 8)
	binary.LittleEndian.PutUint64(byteID, i)
	return byteID
}

func uint64String(i uint64) string {
	return string(uint64Bytes(i))
}

func int32Bytes(i int32) []byte {
	return []byte{byte(0xff & i),
		byte(0xff & (i >> 8)),
		byte(0xff & (i >> 16)),
		byte(0xff & (i >> 24))}
}

func bytesInt32(b []byte) int32 {
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return int32(b[0]) |
		int32(b[1])<<8 |
		int32(b[2])<<16 |
		int32(b[3])<<24
}
