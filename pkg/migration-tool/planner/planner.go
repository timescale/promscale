package planner

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"os"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

// Plan represents the plannings done by the planner.
type Plan struct {
	mux  sync.RWMutex
	Mint int64
	Maxt int64
	//queryURL string // URL for query endpoint at the remote-write storage.
	//pMetricName string // Metric for tracking the last pushed max timestamp to remote-write storage.
	readClient         remote.ReadClient // Used to fetch last maxt pushed.
	progressMetricName string
	currentBlock       *Block
	blockCounts        atomic.Int64
	lastPushedT        atomic.Int64
}

// CreatePlan creates a planner in the provided planner-path. It checks for planner config in the
// provided planner path. If found, it loads the config and verifies the validity. If found invalid,
// a new planner config is created with updated details. Older planner config and related data are
// deleted if the validity is unable to hold true.
func CreatePlan(mint, maxt int64, progressMetricName, remoteWriteStorageReadURL string) (*Plan, bool, error) {
	rc, err := utils.CreateReadClient("reader-last-maxt-pushed", remoteWriteStorageReadURL, model.Duration(time.Minute*2))
	if err != nil {
		return nil, false, fmt.Errorf("create last-pushed-maxt reader: %w", err)
	}
	plan := &Plan{
		Mint:               mint,
		Maxt:               maxt,
		readClient:         rc,
		progressMetricName: progressMetricName,
	}
	plan.blockCounts.Store(0)
	plan.lastPushedT.Store(0)
	if remoteWriteStorageReadURL != "" {
		lastPushedMaxt, found, err := plan.fetchLastPushedMaxt()
		if err != nil {
			return nil, false, fmt.Errorf("create plan: %w", err)
		}
		if found && lastPushedMaxt > plan.Mint && lastPushedMaxt < plan.Maxt {
			log.Warn("msg", fmt.Sprintf("progress-metric found on the write storage. Last push was on %d."+
				"Resuming from mint: %d to maxt: %d time-range: %d mins", lastPushedMaxt, lastPushedMaxt+1, plan.Maxt, (plan.Maxt-lastPushedMaxt+1)/time.Minute.Milliseconds()))
			plan.Mint = lastPushedMaxt + 1
		} else {
			log.Warn("msg", "progress-metric not found on the write storage. Continuing with the provided mint and maxt.")
		}
	}
	if plan.Mint >= plan.Maxt {
		log.Info("msg", "mint greater than or equal to maxt. This given time-range migration has already been carried out. Hence, stopping.")
	}
	return plan, true, nil
}

// fetchLastPushedMaxt fetches the maxt of the last block pushed to remote-write storage. At present, this is developed
// for a single migration job (i.e., not supporting multiple migration metrics).
func (p *Plan) fetchLastPushedMaxt() (lastPushedMaxt int64, found bool, err error) {
	query, err := utils.CreatePrombQuery(p.Mint, p.Maxt, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, p.progressMetricName)})
	if err != nil {
		return -1, false, fmt.Errorf("fetch-last-pushed-maxt create promb query: %w", err)
	}
	result, err := p.readClient.Read(context.Background(), query)
	if err != nil {
		return -1, false, fmt.Errorf("fetch-last-pushed-maxt query result: %w", err)
	}
	ts := result.Timeseries
	if len(ts) == 0 {
		return -1, false, nil
	}
	for _, series := range ts {
		for i := len(series.Samples) - 1; i >= 0; i-- {
			if series.Samples[i].Timestamp > lastPushedMaxt {
				lastPushedMaxt = series.Samples[i].Timestamp
			}
		}
	}
	if lastPushedMaxt == 0 {
		return -1, false, nil
	}
	return lastPushedMaxt, true, nil
}

// GetRecentBlockPushed returns the metadata of the most recent block that was pushed to the remote write storage.
func (p *Plan) LastPushedMaxt() int64 {
	return p.lastPushedT.Load()
}

func (p *Plan) UpdateLastPushedMaxt(t int64) {
	p.lastPushedT.Store(t)
}

func (p *Plan) DecrementBlockCount() {
	p.blockCounts.Sub(1)
}

// CreateBlock creates a new block and returns reference to the block for faster write and read operations.
func (p *Plan) CreateBlock(mint, maxt int64) (reference *Block) {
	percent := float64(maxt-p.Mint) * 100 / float64(p.Maxt-p.Mint)
	if percent > 100 {
		percent = 100
	}
	reference = &Block{
		ID:              p.blockCounts.Add(1),
		percent:         percent,
		pbarInitDetails: fmt.Sprintf("block-%d time-range: %d mins | mint: %d | maxt: %d", p.blockCounts.Load(), (maxt-mint)/time.Minute.Milliseconds(), mint, maxt),
		pbar: progressbar.NewOptions(
			6,
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
		),
		mint: mint,
		maxt: maxt,
	}
	reference.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", (maxt-mint)/time.Minute.Milliseconds()), 1)
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
func (p *Plan) Clear() error {
	p.mux.Lock()
	defer p.mux.Unlock()
	if !p.currentBlock.done {
		if err := p.currentBlock.Done(); err != nil {
			return fmt.Errorf("clear: %w", err)
		}
	}
	p.currentBlock = nil
	return nil
}

// Block provides the metadata of a block.
type Block struct {
	ID              int64
	mint            int64
	maxt            int64
	done            bool
	percent         float64
	pbarInitDetails string
	pbar            *progressbar.ProgressBar
	Timeseries      []*prompb.TimeSeries
}

func (b *Block) SetData(timeseries []*prompb.TimeSeries) {
	b.Timeseries = timeseries
}

func (b *Block) SetDescription(description string, proceed int) {
	_ = b.pbar.Add(proceed)
	b.pbar.Describe(fmt.Sprintf("progress: %.3f%% | %s | %s", b.percent, b.pbarInitDetails, description))
}

// Done updates the text and sets the spinner to done.
func (b *Block) Done() error {
	b.done = true
	b.SetDescription("pushed", 1)
	if err := b.pbar.Finish(); err != nil {
		return fmt.Errorf("bar-done: %w", err)
	}
	return nil
}

func (b *Block) Maxt() int64 {
	return b.maxt
}
