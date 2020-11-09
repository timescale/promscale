package planner

import (
	"encoding/json"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"
	"sync"
)

// Plan represents the plannings done by the planner.
type Plan struct {
	mux          sync.RWMutex
	Mint         int64
	Maxt         int64
	currentBlock *Block
	blockCounts  atomic.Int64
	lastPushedT  atomic.Int64
}

// CreatePlan creates a planner in the provided planner-path. It checks for planner config in the
// provided planner path. If found, it loads the config and verifies the validity. If found invalid,
// a new planner config is created with updated details. Older planner config and related data are
// deleted if the validity is unable to hold true.
func CreatePlan(mint, maxt int64) (*Plan, error) {
	plan := &Plan{
		Mint: mint,
		Maxt: maxt,
	}
	plan.blockCounts.Store(0)
	plan.lastPushedT.Store(0)
	return plan, nil
}

// GetRecentBlockPushed returns the metadata of the most recent block that was pushed to the remote write storage.
func (p *Plan) LastPushedMaxt() int64 {
	return p.lastPushedT.Load()
}

// Marshal marshals the plan-meta into byte format.
func (p *Plan) marshal() ([]byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// CreateBlock creates a new block and returns reference to the block for faster write and read operations.
func (p *Plan) CreateBlock(mint, maxt int64) (reference *Block) {
	reference = &Block{
		ID:   p.blockCounts.Add(1),
		Mint: mint,
		Maxt: maxt,
	}
	p.currentBlock = reference
	return
}

// CurrentBlock returns the current in-memory block that is opened for read/write operations.
func (p *Plan) CurrentBlock() *Block {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.currentBlock
}

// Clear clears the current in-memory block and dereferences it. This makes the block accessible to
// garbage collector for freeing the memory.
func (p *Plan) Clear() {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.currentBlock = nil
}

// Block provides the metadata of a block.
type Block struct {
	ID         int64
	Mint       int64
	Maxt       int64
	Timeseries []*prompb.TimeSeries
}

func (b *Block) SetData(timeseries []*prompb.TimeSeries) {
	b.Timeseries = timeseries
}
