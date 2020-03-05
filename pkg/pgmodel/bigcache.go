package pgmodel

import (
	"fmt"

	"github.com/allegro/bigcache"
	"github.com/prometheus/prometheus/prompb"
)

var (
	// ErrEntryNotFound is returned when entry is not found.
	ErrEntryNotFound = fmt.Errorf("entry not found")
)

type bCache struct {
	labels *bigcache.BigCache
	series *bigcache.BigCache
}

func (b *bCache) GetLabel(label *prompb.Label) (int32, error) {
	result, err := b.labels.Get(label.String())
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return bytesInt32(result), nil
}

func (b *bCache) SetLabel(label *prompb.Label, id int32) error {
	return b.labels.Set(label.String(), int32Bytes(id))
}

func (b *bCache) GetSeries(fingerprint uint64) (uint64, error) {
	result, err := b.series.Get(uint64String(fingerprint))
	if err != nil {
		if err == bigcache.ErrEntryNotFound {
			return 0, ErrEntryNotFound
		}
		return 0, err
	}
	return bytesUint64(result), nil
}

func (b *bCache) SetSeries(fingerprint, id uint64) error {
	return b.series.Set(uint64String(fingerprint), uint64Bytes(id))
}

func uint64Bytes(i uint64) []byte {
	return []byte{byte(0xff & i),
		byte(0xff & (i >> 8)),
		byte(0xff & (i >> 16)),
		byte(0xff & (i >> 24)),
		byte(0xff & (i >> 32)),
		byte(0xff & (i >> 40)),
		byte(0xff & (i >> 48)),
		byte(0xff & (i >> 56))}
}

func uint64String(i uint64) string {
	return string(uint64Bytes(i))
}

func bytesUint64(b []byte) uint64 {
	_ = b[7] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[0]) |
		uint64(b[1])<<8 |
		uint64(b[2])<<16 |
		uint64(b[3])<<24 |
		uint64(b[4])<<32 |
		uint64(b[5])<<40 |
		uint64(b[6])<<48 |
		uint64(b[7])<<56
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
