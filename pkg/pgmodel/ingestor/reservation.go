package ingestor

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

type reservation struct {
	copySender          <-chan copyRequest
	firstRequestBatched *sync.WaitGroup
	index               int

	lock      sync.Mutex
	startTime time.Time

	items int64
}

func newReservation(cs <-chan copyRequest, startTime time.Time, batched *sync.WaitGroup) *reservation {
	return &reservation{cs, batched, -1, sync.Mutex{}, startTime, 1}
}

func (res *reservation) Update(rq *ReservationQueue, t time.Time, num_insertables int) {
	rest := res.GetStartTime()
	atomic.AddInt64(&res.items, int64(num_insertables))

	if t.Before(rest) {
		//this should happen rarely
		res.SetStartTime(t)
		rq.update(res)
	}
}

func (res *reservation) GetStartTime() time.Time {
	res.lock.Lock()
	defer res.lock.Unlock()
	return res.startTime
}

func (res *reservation) SetStartTime(t time.Time) {
	res.lock.Lock()
	defer res.lock.Unlock()

	//double check that it's before
	if t.Before(res.startTime) {
		res.startTime = t
	}
}

// reservationQueueInternal implements heap.Interface
type reservationQueueInternal []*reservation

func newReservationQueueInternal() *reservationQueueInternal {
	q := make(reservationQueueInternal, 0, 100)
	return &q
}

func (res reservationQueueInternal) Len() int { return len(res) }

func (res reservationQueueInternal) Less(i, j int) bool {
	startTimeI := res[i].GetStartTime()
	startTimeJ := res[j].GetStartTime()
	if startTimeI.Equal(startTimeJ) {
		itemsI := atomic.LoadInt64(&res[i].items)
		itemsJ := atomic.LoadInt64(&res[j].items)
		//prerer metrics with more items because they probably hold up more stuff
		return itemsI > itemsJ
	}
	return res[i].GetStartTime().Before(res[j].GetStartTime())
}

func (res reservationQueueInternal) Swap(i, j int) {
	res[i], res[j] = res[j], res[i]
	res[i].index = i
	res[j].index = j
}

func (res *reservationQueueInternal) Push(x interface{}) {
	n := len(*res)
	item := x.(*reservation)
	item.index = n
	*res = append(*res, item)
}

func (res *reservationQueueInternal) Pop() interface{} {
	old := *res
	n := len(old)
	item := old[n-1]
	item.index = -1 //for safety
	old[n-1] = nil  // avoid memory leak
	*res = old[0 : n-1]
	return item
}

type Reservation interface {
	Update(*ReservationQueue, time.Time, int)
}

type ReservationQueue struct {
	lock   sync.Mutex
	cond   sync.Cond
	q      *reservationQueueInternal
	closed bool
}

func NewReservationQueue() *ReservationQueue {
	res := &ReservationQueue{lock: sync.Mutex{}, q: newReservationQueueInternal(), closed: false}
	res.cond = *sync.NewCond(&res.lock)
	return res
}

func (rq *ReservationQueue) Add(cs <-chan copyRequest, batched *sync.WaitGroup, startTime time.Time) Reservation {
	si := newReservation(cs, startTime, batched)

	rq.lock.Lock()
	defer rq.lock.Unlock()

	if rq.closed {
		panic("Should never add to a closed queue")
	}

	if rq.q.Len() == 0 {
		rq.cond.Broadcast()
	}

	heap.Push(rq.q, si)
	return si
}

func (rq *ReservationQueue) Len() int {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	return rq.q.Len()
}

func (rq *ReservationQueue) Close() {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	rq.closed = true
	rq.cond.Broadcast()
}

// Peek gives the first startTime as well as if the queue is not closed.
// It blocks until there is an element in the queue or it has been closed.
func (rq *ReservationQueue) Peek() (time.Time, bool) {
	reservation, waited, ok := rq.peek()
	if !ok {
		return time.Time{}, false
	}
	if waited {
		/* If this is the first reservation in the queue, wait for the entire request to be batched with a timeout.
		* (timeout is really a safety measure to prevent deadlocks if some metric batcher is full, which is unlikely)*/
		waitch := make(chan struct{})
		go func() {
			reservation.firstRequestBatched.Wait()
			close(waitch)
		}()
		select {
		case <-waitch:
		case <-time.After(250 * time.Millisecond):
		}
	}
	return reservation.GetStartTime(), ok
}

func (rq *ReservationQueue) peek() (*reservation, bool, bool) {
	rq.lock.Lock()
	defer rq.lock.Unlock()
	waited := false
	for !rq.closed && rq.q.Len() == 0 {
		waited = true
		rq.cond.Wait()
	}

	if rq.q.Len() > 0 {
		firstReservation := (*rq.q)[0]
		return firstReservation, waited, true
	}

	//must be closed
	return nil, false, false
}

// PopBatch pops from the queue to populate the batch until either batch is full or the queue is empty.
// never blocks. Returns number of requests pop'ed.
func (rq *ReservationQueue) PopOntoBatch(batch []readRequest) ([]readRequest, int, string) {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	count := 0
	items := int64(0)
	if rq.q.Len() > 0 {
		items = atomic.LoadInt64(&(*rq.q)[0].items)
	}
	total_items := int64(0)
	for len(batch) < cap(batch) && rq.q.Len() > 0 && (len(batch) == 0 || items+total_items < 20000) {
		res := heap.Pop(rq.q).(*reservation)
		batch = append(batch, readRequest{res.copySender})
		count++
		total_items += items
		items = 0
		if rq.q.Len() > 0 {
			items = atomic.LoadInt64(&(*rq.q)[0].items)
		}
	}
	reason := "timeout"
	if !(len(batch) < cap(batch)) {
		reason = "size_metrics"
	} else if !(len(batch) == 0 || items+total_items < 20000) {
		reason = "size_samples"
	}
	return batch, count, reason
}

func (rq *ReservationQueue) update(res *reservation) {
	rq.lock.Lock()
	defer rq.lock.Unlock()
	if res.index < 0 { //has already been poped
		return
	}
	heap.Fix(rq.q, res.index)
}
