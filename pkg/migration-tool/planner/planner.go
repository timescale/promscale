package planner

import (
	"context"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/atomic"
	"math"
	"os"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

const (
	MaxTimeRangeDeltaLimit    = time.Minute * 120
	ResponseDataSizeHalfLimit = utils.Megabyte * 25
)

// Plan represents the plannings done by the planner.
type Plan struct {
	Mint               int64
	Maxt               int64
	readClient         *utils.Client // Used to fetch last maxt pushed from write storage.
	jobName            string
	progressMetricName string       // Name for progress metric.
	blockCounts        atomic.Int64 // Used in maintaining the ID of the in-memory blocks.
	// Block time-range vars.
	lastMaxT           int64
	lastNumBytes       int
	lastTimeRangeDelta int64
}

// CreatePlan creates an in-memory planner. It is responsible for fetching the last pushed maxt and based on that, updates
// the mint for the provided migration.
func CreatePlan(mint, maxt int64, progressMetricName, jobName, remoteWriteStorageReadURL string, progressEnabled bool) (*Plan, bool, error) {
	rc, err := utils.CreateReadClient("reader-last-maxt-pushed", remoteWriteStorageReadURL, model.Duration(time.Minute*2))
	if err != nil {
		return nil, false, fmt.Errorf("create last-pushed-maxt reader: %w", err)
	}
	plan := &Plan{
		Mint:               mint,
		Maxt:               maxt,
		readClient:         rc,
		jobName:            jobName,
		progressMetricName: progressMetricName,
	}
	plan.blockCounts.Store(0)
	if progressEnabled && remoteWriteStorageReadURL == "" {
		return nil, false, fmt.Errorf("read url for remote-write storage should be provided when progress metric is enabled")
	}
	if !progressEnabled {
		log.Info("msg", "Resuming from where we left off is turned off. Starting at the beginning of the provided time-range.")
	}
	var found bool
	if remoteWriteStorageReadURL != "" && progressEnabled {
		lastPushedMaxt, found, err := plan.fetchLastPushedMaxt()
		if err != nil {
			return nil, false, fmt.Errorf("create plan: %w", err)
		}
		if found && lastPushedMaxt > plan.Mint && lastPushedMaxt <= plan.Maxt {
			plan.Mint = lastPushedMaxt + 1
			log.Warn("msg", fmt.Sprintf("Resuming from where we left off. Last push was on %d. "+
				"Resuming from mint: %d to maxt: %d time-range: %d mins", lastPushedMaxt, plan.Mint, plan.Maxt, (plan.Maxt-lastPushedMaxt+1)/time.Minute.Milliseconds()))
		}
	}
	plan.lastMaxT = math.MinInt64
	if plan.Mint > plan.Maxt {
		return nil, false, fmt.Errorf("invalid: mint greater than maxt")
	}
	if plan.Mint == plan.Maxt && !found {
		// progress_metric does not exists. This means that the migration is expected to be an instant query.
		log.Info("msg", "mint equal to maxt. Considering this as instant query.")
		return plan, true, nil
	}

	return plan, true, nil
}

// fetchLastPushedMaxt fetches the maxt of the last block pushed to remote-write storage. At present, this is developed
// for a single migration job (i.e., not supporting multiple migration metrics and successive migrations).
func (p *Plan) fetchLastPushedMaxt() (lastPushedMaxt int64, found bool, err error) {
	query, err := utils.CreatePrombQuery(p.Mint, p.Maxt, []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, p.progressMetricName),
		labels.MustNewMatcher(labels.MatchEqual, utils.LabelJob, p.jobName),
	})
	if err != nil {
		return -1, false, fmt.Errorf("fetch-last-pushed-maxt create promb query: %w", err)
	}
	result, _, _, err := p.readClient.Read(context.Background(), query)
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

// Update updates the details of the planner that are dependent on previous fetch stats.
func (p *Plan) Update(numBytes int) {
	p.lastNumBytes = numBytes
}

// NewBlock returns a new block after allocating the time-range for fetch.
func (p *Plan) NextBlock() (reference *Block, err error, delta int64) {
	timeDelta := determineTimeDelta(p.lastNumBytes, p.lastTimeRangeDelta)
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
		return nil, fmt.Errorf("next-block: %w", err), timeDelta
	}
	return bRef, nil, timeDelta
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
		id:              id,
		percent:         percent,
		pbarInitDetails: baseDescription,
		pbar: progressbar.NewOptions(
			6,
			progressbar.OptionOnCompletion(func() {
				_, _ = fmt.Fprint(os.Stderr, "\n")
			}),
		),
		mint: mint,
		maxt: maxt,
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

func determineTimeDelta(numBytes int, prevTimeDelta int64) int64 {
	// TODO: add explanation of logic as an overview.
	switch {
	case numBytes == ResponseDataSizeHalfLimit:
		return prevTimeDelta
	case numBytes < ResponseDataSizeHalfLimit && prevTimeDelta == MaxTimeRangeDeltaLimit.Milliseconds():
		// The balanced scenario where the time-range is neither too long to cause OOM nor too less
		// to slow down the migration process.
		// An important thing to note is that the priority of size is more than that of time. That means,
		// if both bytes and time are greater than the respective limits, then we down size the time so that
		// bytes size can be controlled.
		return prevTimeDelta
	case numBytes < ResponseDataSizeHalfLimit && prevTimeDelta > MaxTimeRangeDeltaLimit.Milliseconds():
		// Though the bytes count is less than the limit, the time range delta is more, hence we leave here
		// unchanged so that it can be reset back to the MaxTimeRangeDeltaLimit.
	case numBytes < ResponseDataSizeHalfLimit:
		// We increase the time-range linearly for the next fetch if the current time-range fetch resulted in size that is
		// less than half the aimed maximum size of the in-memory block. This continues till we reach the
		// maximum time-range delta.
		prevTimeDelta += time.Minute.Milliseconds()
	default:
		// Decrease the time-range delta exponentially if the current fetch size exceeds the half aimed. This is done so that
		// we reach an ideal time-range as the block numbers grow and successively reach the balanced scenario.
		return prevTimeDelta / 2
	}
	if prevTimeDelta > MaxTimeRangeDeltaLimit.Milliseconds() {
		// Reset the time delta to the maximum permittable value.
		prevTimeDelta = MaxTimeRangeDeltaLimit.Milliseconds()
	}
	return prevTimeDelta
}
