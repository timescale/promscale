package planner

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"math"
	"os"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

var (
	minute                 = time.Minute.Milliseconds()
	laIncrement            = minute
	maxTimeRangeDeltaLimit = minute * 120
)

// Plan represents the plannings done by the planner.
type Plan struct {
	Mint                      int64
	Maxt                      int64
	BlockSizeLimitBytes       int64
	JobName                   string
	ProgressMetricName        string // Name for progress metric.
	ProgressEnabled           bool
	RemoteWriteStorageReadURL string
	blockCounts               atomic.Int64 // Used in maintaining the ID of the in-memory blocks.
	// Block time-range vars.
	lastMaxT              int64
	lastNumBytes          atomic.Int64
	lastTimeRangeDelta    int64
	permittableSizeBounds int64
	pbarMux               *sync.Mutex
	IsTest                bool
}

// InitPlan creates an in-memory planner and initializes it. It is responsible for fetching the last pushed maxt and based on that, updates
// the mint for the provided migration.
func Init(config *Plan) (bool, error) {
	config.pbarMux = new(sync.Mutex)
	config.blockCounts.Store(0)
	if config.ProgressEnabled && config.RemoteWriteStorageReadURL == "" {
		return false, fmt.Errorf("read url for remote-write storage should be provided when progress metric is enabled")
	}
	if !config.ProgressEnabled {
		log.Info("msg", "Resuming from where we left off is turned off. Starting at the beginning of the provided time-range.")
	}
	var found bool
	if config.RemoteWriteStorageReadURL != "" && config.ProgressEnabled {
		lastPushedMaxt, found, err := config.fetchLastPushedMaxt()
		if err != nil {
			return false, fmt.Errorf("create plan: %w", err)
		}
		if found && lastPushedMaxt > config.Mint && lastPushedMaxt <= config.Maxt {
			config.Mint = lastPushedMaxt + 1
			log.Warn("msg", fmt.Sprintf("Resuming from where we left off. Last push was on %d. "+
				"Resuming from mint: %d to maxt: %d time-range: %d mins", lastPushedMaxt, config.Mint, config.Maxt, (config.Maxt-lastPushedMaxt+1)/time.Minute.Milliseconds()))
		}
	}
	config.lastMaxT = math.MinInt64
	if config.Mint >= config.Maxt && found {
		log.Info("msg", "mint greater than or equal to maxt. Migration has already been carried out.")
		return false, nil
	} else if config.Mint >= config.Maxt && !found {
		// Extra sanitary check, even though this will be caught by the validateConf().
		return false, fmt.Errorf("mint cannot be greater than maxt: mint %d maxt %d", config.Mint, config.Mint)
	}
	config.permittableSizeBounds = config.BlockSizeLimitBytes / 20 // 5% of the total block size limit.
	return true, nil
}

// fetchLastPushedMaxt fetches the maxt of the last block pushed to remote-write storage. At present, this is developed
// for a single migration job (i.e., not supporting multiple migration metrics and successive migrations).
func (p *Plan) fetchLastPushedMaxt() (lastPushedMaxt int64, found bool, err error) {
	query, err := utils.CreatePrombQuery(p.Mint, p.Maxt, []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, p.ProgressMetricName),
		labels.MustNewMatcher(labels.MatchEqual, utils.LabelJob, p.JobName),
	})
	if err != nil {
		return -1, false, fmt.Errorf("fetch-last-pushed-maxt create promb query: %w", err)
	}
	readClient, err := utils.NewClient("reader-last-maxt-pushed", p.RemoteWriteStorageReadURL, utils.Write, model.Duration(time.Minute*2))
	if err != nil {
		return -1, false, fmt.Errorf("create fetch-last-pushed-maxt reader: %w", err)
	}
	result, _, _, err := readClient.Read(context.Background(), query)
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

func (p *Plan) DecrementBlockCount() {
	p.blockCounts.Sub(1)
}

// ShouldProceed reports whether the fetching process should proceeds further. If any time-range is left to be
// fetched from the provided time-range, it returns true, else false.
func (p *Plan) ShouldProceed() bool {
	return p.lastMaxT < p.Maxt
}

// update updates the details of the planner that are dependent on previous fetch stats.
func (p *Plan) update(numBytes int) {
	p.lastNumBytes.Store(int64(numBytes))
}

// NewBlock returns a new block after allocating the time-range for fetch.
func (p *Plan) NextBlock() (reference *Block, err error) {
	timeDelta := determineTimeDelta(p.lastNumBytes.Load(), p.BlockSizeLimitBytes, p.permittableSizeBounds, p.lastTimeRangeDelta)
	var mint int64
	if p.lastMaxT == math.MinInt64 {
		mint = p.Mint
	} else {
		mint = p.lastMaxT + 1
	}
	maxt := mint + timeDelta
	if maxt > p.Maxt {
		maxt = p.Maxt
	}
	p.lastMaxT = maxt
	p.lastTimeRangeDelta = timeDelta
	bRef, err := p.createBlock(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("next-block: %w", err)
	}
	return bRef, nil
}

// createBlock creates a new block and returns reference to the block for faster write and read operations.
func (p *Plan) createBlock(mint, maxt int64) (reference *Block, err error) {
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
		id:                    id,
		percent:               percent,
		pbarDescriptionPrefix: baseDescription,
		pbar: progressbar.NewOptions(
			6,
			progressbar.OptionOnCompletion(func() {
				_, _ = fmt.Fprint(os.Stderr, "\n")
			}),
		),
		mint:    mint,
		maxt:    maxt,
		pbarMux: p.pbarMux,
		plan:    p,
	}
	if p.IsTest {
		reference.pbar = nil
		reference.pbarDescriptionPrefix = ""
	}
	reference.SetDescription(fmt.Sprintf("fetching time-range: %d mins...", timeRangeInMinutes), 1)
	return
}

func (p *Plan) validateT(mint, maxt int64) error {
	switch {
	case p.Mint > mint || p.Maxt < mint:
		return fmt.Errorf("invalid mint: %d: global-mint: %d and global-maxt: %d", mint, p.Mint, p.Maxt)
	case p.Mint > maxt || p.Maxt < maxt:
		return fmt.Errorf("invalid maxt: %d: global-mint: %d and global-maxt: %d", mint, p.Mint, p.Maxt)
	case mint > maxt:
		return fmt.Errorf("mint cannot be greater than maxt: mint: %d and maxt: %d", mint, maxt)
	}
	return nil
}

func determineTimeDelta(numBytes, limit, permittable int64, prevTimeDelta int64) int64 {
	switch {
	case numBytes < limit:
		// We increase the time-range linearly for the next fetch if the current time-range fetch resulted in size that is
		// less than half the aimed maximum size of the in-memory block. This continues till we reach the
		// maximum time-range delta.
		if prevTimeDelta >= maxTimeRangeDeltaLimit {
			return maxTimeRangeDeltaLimit
		}
		return clampTimeDelta(prevTimeDelta, laIncrement)
	case numBytes > limit && numBytes <= (limit+permittable):
		// We continue the previous time-delta if the increase in numBytes is only 5% more than the BlockSizeHalfLimit.
		// Example: if the size at current pull is 515 MB, decreasing by twice will take to 257 MB, which is much slower.
		// Hence, we allow to continue at that limit till its within the permittable bounds.
		return prevTimeDelta
	case numBytes > limit:
		// The priority of size is more than that of time. That means, even if both bytes and time are greater than
		// the respective limits, then we down size the time so that bytes size can be controlled (preventing OOM).
		log.Info("msg", fmt.Sprintf("decreasing time-range delta to %d minute(s) since size beyond permittable limits", prevTimeDelta/(2*minute)))
		return prevTimeDelta / 2
	}
	// The ideal scenario where the time-range is neither too long to cause OOM nor too less
	// to slow down the migration process.
	return prevTimeDelta
}

func clampTimeDelta(t int64, incr int64) int64 {
	t += incr
	if t > maxTimeRangeDeltaLimit {
		return maxTimeRangeDeltaLimit
	}
	return t
}
