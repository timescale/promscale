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
	Mint               int64
	Maxt               int64
	mux                sync.RWMutex
	readClient         remote.ReadClient // Used to fetch last maxt pushed from write storage.
	progressMetricName string            // Name for progress metric.
	currentBlock       *Block
	blockCounts        atomic.Int64 // Used in maintaining the ID of the in-memory blocks.
	lastPushedT        atomic.Int64 // Maxt of the last pushed in-memory block.
}

// CreatePlan creates an in-memory planner. It is responsible for fetching the last pushed maxt and based on that, updates
// the mint for the provided migration.
func CreatePlan(mint, maxt int64, progressMetricName, remoteWriteStorageReadURL string, ignoreProgress bool) (*Plan, bool, error) {
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
	if ignoreProgress {
		log.Info("msg", "ignoring progress-metric. Continuing migration with the provided time-range.")
	}
	if remoteWriteStorageReadURL != "" && !ignoreProgress {
		lastPushedMaxt, found, err := plan.fetchLastPushedMaxt()
		if err != nil {
			return nil, false, fmt.Errorf("create plan: %w", err)
		}
		if found && lastPushedMaxt > plan.Mint && lastPushedMaxt < plan.Maxt {
			log.Warn("msg", fmt.Sprintf("progress-metric found on the write storage. Last push was on %d. "+
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
// for a single migration job (i.e., not supporting multiple migration metrics and successive migrations).
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

// LastPushedT returns the maximum timestamp of the most recent block that was pushed to the remote write storage.
func (p *Plan) LastPushedT() int64 {
	return p.lastPushedT.Load()
}

func (p *Plan) SetLastPushedT(t int64) {
	p.lastPushedT.Store(t)
}

func (p *Plan) DecrementBlockCount() {
	p.blockCounts.Sub(1)
}

// CreateBlock creates a new block and returns reference to the block for faster write and read operations.
func (p *Plan) CreateBlock(mint, maxt int64) (reference *Block, err error) {
	if err = p.validateT(mint, maxt); err != nil {
		return nil, fmt.Errorf("create-block: %w", err)
	}
	id := p.blockCounts.Add(1)
	timeRangeInMinutes := (maxt - mint) / time.Minute.Milliseconds()
	percent := float64(maxt-p.Mint) * 100 / float64(p.Maxt-p.Mint)
	if percent > 100 {
		percent = 100
	}
	baseDescription := fmt.Sprintf("block-%d time-range: %d mins | mint: %d | maxt: %d", id, timeRangeInMinutes, mint, maxt)
	reference = &Block{
		id:              id,
		percent:         percent,
		pbarInitDetails: baseDescription,
		pbar: progressbar.NewOptions(
			6,
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
		),
		mint: mint,
		maxt: maxt,
	}
	reference.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeInMinutes), 1)
	p.currentBlock = reference
	return
}

func (p *Plan) validateT(mint, maxt int64) error {
	switch {
	case p.Mint > mint || p.Maxt < mint:
		return fmt.Errorf("invalid mint")
	case p.Mint > maxt || p.Maxt < maxt:
		return fmt.Errorf("invalid maxt")
	case mint > maxt:
		return fmt.Errorf("mint cannot be greater than maxt")
	}
	return nil
}

// Clear clears the current in-memory block and dereferences it. This makes the block accessible to
// garbage collector for freeing the memory.
func (p *Plan) Clear() error {
	p.mux.Lock()
	defer p.mux.Unlock()
	if !p.currentBlock.done {
		if err := p.currentBlock.Done(); err != nil {
			return fmt.Errorf("clear current in-memory block: %w", err)
		}
	}
	p.currentBlock = nil
	return nil
}

// Block represents an in-memory storage for data that is fetched by the reader.
type Block struct {
	id              int64
	mint            int64
	maxt            int64
	numBytes        int
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

func (b *Block) SetBytes(num int) {
	b.numBytes = num
}

// Done updates the text and sets the spinner to done.
func (b *Block) Done() error {
	b.done = true
	b.SetDescription(fmt.Sprintf("pushed %.2f MB", float64(b.numBytes)/float64(utils.Megabyte)), 1)
	if err := b.pbar.Finish(); err != nil {
		return fmt.Errorf("finish block-lifecycle: %w", err)
	}
	return nil
}

func (b *Block) Maxt() int64 {
	return b.maxt
}
