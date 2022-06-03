package bench

import (
	"fmt"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

//This is a sample iterator that buffers a chunk's worth of data at a time.
//The buffering is needed because iterating inside the chunk causes io seeks
//because of the memory mapping. It makes it impossible for the benchmarker
//to keep up.
type BufferingIterator struct {
	chunkR        tsdb.ChunkReader
	chunksMeta    []chunks.Meta
	pool          chunkenc.Pool
	chunkIndex    int
	chunk         chunkenc.Chunk
	chunkIterator chunkenc.Iterator
	err           error
}

func NewBufferingIterator(chunkR tsdb.ChunkReader, chunksMeta []chunks.Meta) *BufferingIterator {
	return &BufferingIterator{
		chunkR:     chunkR,
		chunksMeta: chunksMeta,
		pool:       chunkenc.NewPool(),
		chunkIndex: -1,
	}
}

func (bi *BufferingIterator) Debug() {
	t, v := bi.At()
	fmt.Printf("Buffering iterator debug: chunkIndex=%v len=%v T=%v V=%v \n", bi.chunkIndex, len(bi.chunksMeta), t, v)
	fmt.Printf("Buffering iterator meta debug current: min=%v max=%v \n", bi.chunksMeta[bi.chunkIndex].MinTime, bi.chunksMeta[bi.chunkIndex].MaxTime)
	if bi.chunkIndex > 0 {
		fmt.Printf("Buffering iterator meta debug prev: min=%v max=%v \n", bi.chunksMeta[bi.chunkIndex-1].MinTime, bi.chunksMeta[bi.chunkIndex-1].MaxTime)
	}

}

func (bi *BufferingIterator) nextChunk() bool {
	if bi.chunkIndex >= len(bi.chunksMeta)-1 {
		return false
	}
	bi.chunkIndex++

	chk, err := bi.chunkR.Chunk(bi.chunksMeta[bi.chunkIndex].Ref)
	if err != nil {
		bi.err = err
		return false
	}

	encoding := chk.Encoding()
	dataOriginal := chk.Bytes()
	data := make([]byte, len(dataOriginal))
	//this is the key part, we are copying the bytes
	copy(data, dataOriginal)

	if bi.chunk != nil {
		if bi.chunkIterator.Next() {
			panic("old chunk isn't exhausted")
		}
		bi.chunkIterator = nil
		bi.pool.Put(bi.chunk)
	}

	bi.chunk, err = bi.pool.Get(encoding, data)
	if err != nil {
		bi.err = err
		return false
	}

	bi.chunkIterator = bi.chunk.Iterator(bi.chunkIterator)
	return bi.chunkIterator.Next()
}

func (bi *BufferingIterator) Next() bool {
	if bi.err != nil {
		return false
	}

	if bi.chunkIterator != nil && bi.chunkIterator.Next() {
		return true
	}

	return bi.nextChunk()
}

func (bi *BufferingIterator) At() (int64, float64) {
	return bi.chunkIterator.At()
}

func (bi *BufferingIterator) Seek(t int64) bool {
	panic("not implemeneted")
}

func (bi *BufferingIterator) Err() error {
	return bi.err
}
