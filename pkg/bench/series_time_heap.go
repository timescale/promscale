package bench

import (
	"container/heap"
	"fmt"
	"io"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
)

type SeriesItem struct {
	series_id uint64 // a unique series identifier
	it        chunkenc.Iterator
}

type SeriesTimeHeap []*SeriesItem

func NewSeriesTimeHeap(dm *DataModifier, block *tsdb.Block, qmi *qmInfo, seriesIndex int) (SeriesTimeHeap, []io.Closer, error) {
	sth := make(SeriesTimeHeap, 0, 100)
	fmt.Println("Starting to load series")
	seriesId := uint64(0)
	closers := []io.Closer{}

	ir, err := block.Index()
	if err != nil {
		return nil, closers, err
	}

	p, err := ir.Postings("", "") // The special all key.
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, ir)

	chunkr, err := block.Chunks()
	if err != nil {
		return nil, closers, err
	}
	closers = append(closers, chunkr)

	chks := []chunks.Meta{}
	for p.Next() {
		lbls := labels.Labels{}

		if err = ir.Series(p.At(), &lbls, &chks); err != nil {
			return nil, closers, err
		}

		chksCopy := make([]chunks.Meta, len(chks))
		copy(chksCopy, chks)

		it := NewBufferingIterator(chunkr, chksCopy)
		if !it.Next() { //initialize to first position
			panic("can't get first item")
		}
		si := &SeriesItem{seriesId, it}
		sth = append(sth, si)

		dm.VisitSeries(seriesId, lbls, func(rec record.RefSeries) {
			seriesId++
			qmi.qm.StoreSeries([]record.RefSeries{rec}, seriesIndex)
		})
	}
	fmt.Println("Done loading series, # series", seriesId)

	if err := p.Err(); err != nil {
		return nil, closers, err
	}

	heap.Init(&sth)
	return sth, closers, nil
}

func (pq SeriesTimeHeap) Len() int { return len(pq) }

func (pq SeriesTimeHeap) Less(i, j int) bool {
	tsi, _ := pq[i].it.At()
	tsj, _ := pq[j].it.At()
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

func (pq *SeriesTimeHeap) Visit(dm *DataModifier, visitor func([]record.RefSample, int64) error) error {
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
