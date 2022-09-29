package bench

import (
	"container/heap"
	"fmt"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type SeriesItem struct {
	series_id uint64 // a unique series identifier
	it        chunkenc.Iterator
}

type SeriesTimeHeap []*SeriesItem

func NewSeriesTimeHeap() *SeriesTimeHeap {
	sth := make(SeriesTimeHeap, 0, 100)
	return &sth
}

func (pq *SeriesTimeHeap) Add(seriesID uint64, it *BufferingIterator) {
	if !it.Next() { //initialize to first position
		panic("can't get first item")
	}
	si := &SeriesItem{seriesID, it}
	*pq = append(*pq, si)
}

func (pq SeriesTimeHeap) Len() int { return len(pq) }

func (pq SeriesTimeHeap) Less(i, j int) bool {
	tsi, _ := pq[i].it.At()
	tsj, _ := pq[j].it.At()
	if tsi == tsj {
		return pq[i].series_id < pq[j].series_id
	}
	return tsi < tsj
}

func (pq SeriesTimeHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *SeriesTimeHeap) Push(x interface{}) {
	item := x.(*SeriesItem)
	*pq = append(*pq, item)
}

func (pq *SeriesTimeHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq *SeriesTimeHeap) Debug() {
	item := (*pq)[0]
	seriesID := uint64(item.series_id)
	ts, val := item.it.At()
	fmt.Printf("SeriesTimeHeap Debug seriesID=%v T=%v val=%v\n", seriesID, ts, val)
	bi, ok := item.it.(*BufferingIterator)
	if ok {
		bi.Debug()
	} else {
		fmt.Println("Not buffering iterator")
	}
}

func (pq *SeriesTimeHeap) Run(out chan<- *point) {
	var err error
	err = nil

	defer close(out)

	heap.Init(pq)
	for pq.Len() > 0 {
		item := (*pq)[0]
		//fmt.Printf("%s %g %d\n", item.series.Labels(), item.val, item.ts)

		seriesID := uint64(item.series_id)
		ts, val := item.it.At()
		pt := &point{seriesID, ts, val, err}
		out <- pt

		if item.it.Next() {
			heap.Fix(pq, 0)
		} else {
			err = item.it.Err()
			item2 := heap.Pop(pq)
			if item != item2 {
				panic("items not equal")
			}
		}
	}
}

/*func (pq *SeriesTimeHeap) Visit(dm *DataModifier, visitor func([]record.RefSample, int64) error) error {
	for pq.Len() > 0 {
		item := (*pq)[0]
		//fmt.Printf("%s %g %d\n", item.series.Labels(), item.val, item.ts)

		seriesID := uint64(item.series_id)
		ts, val := item.it.At()
		dm.VisitSamples(seriesID, ts, val, visitor)

		if item.it.Next() {
			heap.Fix(pq, 0)
		} else {
			if err := item.it.Err(); err != nil {
				return err
			}
			item2 := heap.Pop(pq)
			if item != item2 {
				panic("items not equal")
			}
		}
	}
	return nil
}
*/
