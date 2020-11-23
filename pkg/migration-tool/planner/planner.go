package planner

import (
	"context"
	"fmt"
	"go.uber.org/atomic"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/remote"
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
	readClient         remote.ReadClient // Used to fetch last maxt pushed from write storage.
	progressMetricName string            // Name for progress metric.
	currentBlock       *Block
	blockCounts        atomic.Int64 // Used in maintaining the ID of the in-memory blocks.
	lastPushedT        atomic.Int64 // Maxt of the last pushed in-memory block.
	// Block time-range vars.
	lastMaxT           int64
	lastNumBytes       int
	lastTimeRangeDelta int64
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
		if found && lastPushedMaxt > plan.Mint && lastPushedMaxt <= plan.Maxt {
			log.Warn("msg", fmt.Sprintf("progress-metric found on the write storage. Last push was on %d. "+
				"Resuming from mint: %d to maxt: %d time-range: %d mins", lastPushedMaxt, lastPushedMaxt+1, plan.Maxt, (plan.Maxt-lastPushedMaxt+1)/time.Minute.Milliseconds()))
			plan.Mint = lastPushedMaxt + 1
		}
	}
	if plan.Mint >= plan.Maxt {
		log.Info("msg", "mint greater than or equal to maxt. This given time-range migration has already been carried out. Hence, stopping.")
	}
	plan.lastMaxT = plan.Mint - 1 // Since block creation starts with (lastMaxT + 1), the first block will miss one millisecond if we do not subtract from here.
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
	mint := p.lastMaxT + 1
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
