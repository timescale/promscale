package bench

import (
	"container/heap"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

//This is a sample iterator that buffers a chunk's worth of data at a time.
//The buffering is needed because iterating inside the chunk causes io seeks
//because of the memory mapping. It makes it impossible for the benchmarker
//to keep up.

var pool = chunkenc.NewPool()
var needNextChunks int32 = 0
var enqueued int32 = 0
var dequeued int32 = 0
var fetchChunks int32 = 0
var waitInRotate int32 = 0
var asyncFetchChunks int32 = 0
var syncFetchChunks int32 = 0

type BufferingIteratorHeap []*BufferingIterator

func NewBufferingIteratorHeap() BufferingIteratorHeap {
	bit := BufferingIteratorHeap(make([]*BufferingIterator, 0))
	heap.Init(&bit)
	return bit
}

func (pq BufferingIteratorHeap) Len() int { return len(pq) }

func (pq BufferingIteratorHeap) Less(i, j int) bool {
	return pq[i].chunksMeta[pq[i].chunkIndex+1].MinTime < pq[j].chunksMeta[pq[j].chunkIndex+1].MinTime
}

func (pq *BufferingIteratorHeap) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
	(*pq)[i].queueIndex = i
	(*pq)[j].queueIndex = j
}

func (pq *BufferingIteratorHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*BufferingIterator)
	item.queueIndex = n
	*pq = append(*pq, item)
}

func (pq *BufferingIteratorHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.queueIndex = -1
	*pq = old[0 : n-1]
	return item
}

var qL = sync.Mutex{}
var qC = sync.NewCond(&qL)
var q BufferingIteratorHeap = NewBufferingIteratorHeap()

//todo move out of init
func init() {
	chunkFetchWorkers := 10
	//todo close channel && wg
	for i := 0; i < chunkFetchWorkers; i++ {
		go chunkFetchWorker()
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

	atomic.AddInt32(&fetchChunks, 1)
	return pool.Get(encoding, data)
}

func chunkFetchWorker() {
	for {
		qC.L.Lock()
		for len(q) == 0 {
			qC.Wait()
		}
		request := q[0]
		request.dequeue()
		qC.L.Unlock()

		chk, err := fetchChunk(request.chunkR, request.chunksMeta[request.chunkIndex+1])
		if err != nil {
			panic(err)
		}
		if len(request.nextChunkCh) != 0 {
			panic("next chan is full async")
		}
		request.nextChunkCh <- chk
		atomic.AddInt32(&asyncFetchChunks, 1)
	}
}

type BufferingIterator struct {
	seriesID      uint64
	chunkR        tsdb.ChunkReader
	chunksMeta    []chunks.Meta
	chunkIndex    int
	chunk         chunkenc.Chunk
	nextChunkCh   chan chunkenc.Chunk
	queueIndex    int
	chunkIterator chunkenc.Iterator
	err           error
}

func NewBufferingIterator(seriesID uint64, chunkR tsdb.ChunkReader, chunksMeta []chunks.Meta) *BufferingIterator {
	bi := &BufferingIterator{
		seriesID:    seriesID,
		chunkR:      chunkR,
		chunksMeta:  chunksMeta,
		chunkIndex:  -1,
		nextChunkCh: make(chan chunkenc.Chunk, 1),
	}
	if bi.nextChunkExists() {
		atomic.AddInt32(&needNextChunks, 1)

		qC.L.Lock()
		bi.enqueue()
		qC.L.Unlock()
	}
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

func (bi *BufferingIterator) isInQueue() bool {
	return bi.queueIndex >= 0
}

func (bi *BufferingIterator) enqueue() {
	atomic.AddInt32(&enqueued, 1)
	heap.Push(&q, bi)
	if len(q) == 1 {
		qC.Broadcast()
	}
}

func (bi *BufferingIterator) dequeue() {
	atomic.AddInt32(&dequeued, 1)
	heap.Remove(&q, bi.queueIndex)
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

	if bi.isInQueue() {
		qC.L.Lock()
		bi.dequeue()
		qC.L.Unlock()

		chk, err := fetchChunk(bi.chunkR, bi.chunksMeta[bi.chunkIndex+1])
		if err != nil {
			panic(err)
		}
		if len(bi.nextChunkCh) != 0 {
			panic("next chan is full sync")
		}
		bi.nextChunkCh <- chk
		atomic.AddInt32(&syncFetchChunks, 1)
	}
	select {
	case bi.chunk = <-bi.nextChunkCh:
	default:
		atomic.AddInt32(&waitInRotate, 1)
		bi.chunk = <-bi.nextChunkCh
	}

	bi.chunkIterator = bi.chunk.Iterator(bi.chunkIterator)
	bi.chunkIndex++

	//do this before enquing
	if !bi.chunkIterator.Next() {
		panic("First chunkIterator next call is false")
	}

	if bi.nextChunkExists() {
		atomic.AddInt32(&needNextChunks, 1)

		qC.L.Lock()
		bi.enqueue()
		qC.L.Unlock()
	}
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
	return true
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
