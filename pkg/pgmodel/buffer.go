package pgmodel

import "sync"

const (
	// maximum number of insertDataRequests that should be buffered before the
	// insertHandler flushes to the next layer. We don't want too many as this
	// increases the number of lost writes if the connector dies. This number
	// was chosen arbitrarily.
	flushSize                = 2000
	getCreateMetricsTableSQL = "SELECT table_name FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	finalizeMetricCreation   = "CALL " + catalogSchema + ".finalize_metric_creation()"
	getSeriesIDForLabelSQL   = "SELECT * FROM " + catalogSchema + ".get_or_create_series_id_for_kv_array($1, $2, $3)"
	getEpochSQL              = "SELECT current_epoch FROM " + catalogSchema + ".ids_epoch LIMIT 1"
	maxCopyRequestsPerTxn    = 100
)

// epoch for the ID caches, -1 means that the epoch was not set
type Epoch = int64

type pendingBuffer struct {
	needsResponse []insertDataTask
	batch         SampleInfoIterator
	epoch         Epoch
}

var pendingBuffers = sync.Pool{
	New: func() interface{} {
		pb := new(pendingBuffer)
		pb.needsResponse = make([]insertDataTask, 0)
		pb.batch = NewSampleInfoIterator()
		pb.epoch = -1
		return pb
	},
}

// Report completion of an insert batch to all goroutines that may be waiting
// on it, along with any error that may have occurred.
// This function also resets the pending in preperation for the next batch.
func (p *pendingBuffer) reportResults(err error) {
	for i := 0; i < len(p.needsResponse); i++ {
		p.needsResponse[i].reportResult(err)
	}
}

func (p *pendingBuffer) release() {
	for i := 0; i < len(p.needsResponse); i++ {
		p.needsResponse[i] = insertDataTask{}
	}
	p.needsResponse = p.needsResponse[:0]

	for i := 0; i < len(p.batch.sampleInfos); i++ {
		// nil all pointers to prevent memory leaks
		p.batch.sampleInfos[i] = samplesInfo{}
	}
	p.batch = SampleInfoIterator{sampleInfos: p.batch.sampleInfos[:0]}
	p.batch.ResetPosition()
	p.epoch = -1
	pendingBuffers.Put(p)
}

func (p *pendingBuffer) addReq(req insertDataRequest, epoch Epoch) bool {
	p.addEpoch(epoch)
	p.needsResponse = append(p.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	p.batch.sampleInfos = append(p.batch.sampleInfos, req.data...)
	return len(p.batch.sampleInfos) > flushSize
}

func (p *pendingBuffer) addEpoch(epoch Epoch) {
	if p.epoch == -1 || epoch < p.epoch {
		p.epoch = epoch
	}
}

func (p *pendingBuffer) absorb(other *pendingBuffer) {
	p.addEpoch(other.epoch)
	p.needsResponse = append(p.needsResponse, other.needsResponse...)
	p.batch.sampleInfos = append(p.batch.sampleInfos, other.batch.sampleInfos...)
}
