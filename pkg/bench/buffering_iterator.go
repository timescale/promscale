package bench

import (
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

//This is a sample iterator that buffers a chunk's worth of data at a time.
//The buffering is needed because iterating inside the chunk causes io seeks
//because of the memory mapping. It makes it impossible for the benchmarker
//to keep up.
type BufferingIterator struct {
	chunkR     tsdb.ChunkReader
	chunksMeta []chunks.Meta
	chunkIndex int
	tsSlice    []int64
	valSlice   []float64
	valIndex   int
	err        error
}

func NewBufferingIterator(chunkR tsdb.ChunkReader, chunksMeta []chunks.Meta) *BufferingIterator {
	return &BufferingIterator{
		chunkR:     chunkR,
		chunksMeta: chunksMeta,
		chunkIndex: -1,
		tsSlice:    make([]int64, 0, 120),
		valSlice:   make([]float64, 0, 120),
	}
}

func (bi *BufferingIterator) fillVals() {
	chk, err := bi.chunkR.Chunk(bi.chunksMeta[bi.chunkIndex].Ref)
	if err != nil {
		bi.err = err
		return
	}

	bi.tsSlice = bi.tsSlice[:0]
	bi.valSlice = bi.valSlice[:0]
	bi.valIndex = 0

	it := chk.Iterator(nil)
	for it.Next() {
		ts, val := it.At()
		bi.tsSlice = append(bi.tsSlice, ts)
		bi.valSlice = append(bi.valSlice, val)
	}

	bi.err = it.Err()
}

func (bi *BufferingIterator) Next() bool {
	if bi.err != nil {
		return false
	}

	if bi.valIndex < (len(bi.tsSlice) - 1) {
		bi.valIndex++
		return true
	}

	if bi.chunkIndex < (len(bi.chunksMeta) - 1) {
		bi.chunkIndex++
		bi.fillVals()
		return bi.err == nil
	}

	return false
}

func (bi *BufferingIterator) At() (int64, float64) {
	return bi.tsSlice[bi.valIndex], bi.valSlice[bi.valIndex]
}

func (bi *BufferingIterator) Seek(t int64) bool {
	panic("not implemeneted")
}

func (bi *BufferingIterator) Err() error {
	return bi.err
}
