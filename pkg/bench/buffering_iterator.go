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

var pool = chunkenc.NewPool()

type chunkRequest struct {
	chunkR     tsdb.ChunkReader
	chunkMeta  chunks.Meta
	responseCh chan<- chunkenc.Chunk
}

var chunkFetchCh chan chunkRequest

//todo move out of init
func init() {
	chunkFetchWorkers := 10
	chunkFetchCh = make(chan chunkRequest, chunkFetchWorkers)
	//todo close channel && wg
	for i := 0; i < chunkFetchWorkers; i++ {
		go chunkFetchWorker(chunkFetchCh)
	}
}

func fetchChunk(chunkR tsdb.ChunkReader, chunkMeta chunks.Meta) (chunkenc.Chunk, error) {
	chk, err := chunkR.Chunk(chunkMeta.Ref)
	if err != nil {
		return nil, err
	}

	encoding := chk.Encoding()
	dataOriginal := chk.Bytes()
	data := make([]byte, len(dataOriginal))
	//this is the key part, we are copying the bytes
	copy(data, dataOriginal)

	return pool.Get(encoding, data)
}

func chunkFetchWorker(requests <-chan chunkRequest) {
	for request := range requests {
		chk, err := fetchChunk(request.chunkR, request.chunkMeta)
		if err != nil {
			panic(err)
		}
		request.responseCh <- chk
	}
}

type BufferingIterator struct {
	chunkR               tsdb.ChunkReader
	chunksMeta           []chunks.Meta
	chunkIndex           int
	chunk                chunkenc.Chunk
	nextChunkCh          chan chunkenc.Chunk
	nextChunkRequestSent bool
	chunkIterator        chunkenc.Iterator
	err                  error
}

func NewBufferingIterator(chunkR tsdb.ChunkReader, chunksMeta []chunks.Meta) *BufferingIterator {
	bi := &BufferingIterator{
		chunkR:      chunkR,
		chunksMeta:  chunksMeta,
		chunkIndex:  -1,
		nextChunkCh: make(chan chunkenc.Chunk, 1),
	}
	bi.sendNextChunkRequest()
	return bi
}

func (bi *BufferingIterator) Debug() {
	t, v := bi.At()
	fmt.Printf("Buffering iterator debug: chunkIndex=%v len=%v T=%v V=%v \n", bi.chunkIndex, len(bi.chunksMeta), t, v)
	fmt.Printf("Buffering iterator meta debug current: min=%v max=%v \n", bi.chunksMeta[bi.chunkIndex].MinTime, bi.chunksMeta[bi.chunkIndex].MaxTime)
	if bi.chunkIndex > 0 {
		fmt.Printf("Buffering iterator meta debug prev: min=%v max=%v \n", bi.chunksMeta[bi.chunkIndex-1].MinTime, bi.chunksMeta[bi.chunkIndex-1].MaxTime)
	}

}

func (bi *BufferingIterator) nextChunkExists() bool {
	return len(bi.chunksMeta)-1 >= bi.chunkIndex+1
}

func (bi *BufferingIterator) sendNextChunkRequest() {
	if !bi.nextChunkRequestSent && bi.nextChunkExists() {
		select {
		case chunkFetchCh <- chunkRequest{bi.chunkR, bi.chunksMeta[bi.chunkIndex+1], bi.nextChunkCh}:
			bi.nextChunkRequestSent = true
		default:
		}
	}
}

func (bi *BufferingIterator) rotateNextChunk() (bool, error) {
	if !bi.nextChunkExists() {
		return false, nil
	}
	if bi.chunk != nil {
		if bi.chunkIterator.Next() {
			panic("old chunk isn't exhausted")
		}
		pool.Put(bi.chunk)
	}

	if bi.nextChunkRequestSent {
		bi.chunk = <-bi.nextChunkCh
	} else {
		var err error
		bi.chunk, err = fetchChunk(bi.chunkR, bi.chunksMeta[bi.chunkIndex+1])
		if err != nil {
			return false, err
		}
	}
	bi.nextChunkRequestSent = false
	bi.chunkIterator = bi.chunk.Iterator(bi.chunkIterator)
	bi.chunkIndex++
	return true, nil
}

func (bi *BufferingIterator) nextChunk() bool {
	valid, err := bi.rotateNextChunk()
	if err != nil {
		bi.err = err
		return false
	}
	if !valid {
		return false
	}
	return bi.chunkIterator.Next()
}

func (bi *BufferingIterator) Next() bool {
	if bi.err != nil {
		return false
	}

	if bi.chunkIterator != nil && bi.chunkIterator.Next() {
		//only send request if I can fulfill curren sample and haven't already sent one
		if !bi.nextChunkRequestSent {
			bi.sendNextChunkRequest()
		}
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
