// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package planner

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/migration-tool/pkg/utils"
	"github.com/timescale/promscale/pkg/log"
)

const numStepsWriter = 5 // Number of progress steps for the progress-bar of the writer.

var (
	second = time.Second.Milliseconds()
	minute = time.Minute.Milliseconds()
)

// Config represents configuration for the planner.
type Config struct {
	Mint                 int64
	Maxt                 int64
	SlabSizeLimitBytes   int64
	NumStores            int
	ProgressEnabled      bool
	JobName              string
	ProgressMetricName   string // Name for progress metric.
	ProgressClientConfig utils.ClientConfig
	HTTPConfig           config.HTTPClientConfig
	LaIncrement          time.Duration
	MaxReadDuration      time.Duration
	HumanReadableTime    bool
}

// Plan represents the plannings done by the planner.
type Plan struct {
	config *Config

	// Slab configs.
	slabCounts         int64 // Used in maintaining the ID of the in-memory slabs.
	pbarMux            *sync.Mutex
	nextMint           int64
	lastNumBytes       int64
	lastTimeRangeDelta int64
	deltaIncRegion     int64 // Time region for which the time-range delta can continue to increase by laIncrement.

	// Test configs.
	Quiet         bool   // Avoid progress-bars during logs.
	TestCheckFunc func() // Helps peek into planner during testing. It is called at createSlab() to check the stats of the last slab.
}

// Init creates an in-memory planner and initializes it. It is responsible for fetching the last pushed maxt and based on that, updates
// the mint for the provided migration.
func Init(config *Config) (*Plan, bool, error) {
	var found bool
	if config.ProgressEnabled {
		if config.ProgressClientConfig.URL == "" {
			return nil, false, fmt.Errorf("read url for remote-write storage should be provided when progress metric is enabled")
		}

		lastPushedMaxt, found, err := config.fetchLastPushedMaxt()
		if err != nil {
			return nil, false, fmt.Errorf("init plan: %w", err)
		}
		if found && lastPushedMaxt > config.Mint && lastPushedMaxt <= config.Maxt {
			config.Mint = lastPushedMaxt
			timeRange := fmt.Sprintf("time-range: %d mins", (config.Maxt-lastPushedMaxt+1)/minute)
			log.Warn("msg", fmt.Sprintf("Resuming from where we left off. Last push was on %d. "+
				"Resuming from mint: %d to maxt: %d %s", lastPushedMaxt, config.Mint, config.Maxt, timeRange))
		}
	} else {
		log.Info("msg", "Resuming from where we left off is turned off. Starting at the beginning of the provided time-range.")
	}
	if config.Mint >= config.Maxt && found {
		log.Info("msg", "mint greater than or equal to maxt. Migration is already complete.")
		return nil, false, nil
	} else if config.Mint >= config.Maxt && !found {
		// Extra sanitary check, even though this will be caught by validateConf().
		return nil, false, fmt.Errorf("mint cannot be greater than maxt: mint %d maxt %d", config.Mint, config.Mint)
	}
	plan := &Plan{
		config:         config,
		pbarMux:        new(sync.Mutex),
		nextMint:       config.Mint,
		deltaIncRegion: config.SlabSizeLimitBytes / 2, // 50% of the total slab size limit.
	}
	return plan, true, nil
}

// fetchLastPushedMaxt fetches the maxt of the last slab pushed to remote-write storage. At present, this is developed
// for a single migration job (i.e., not supporting multiple migration metrics and successive migrations).
func (c *Config) fetchLastPushedMaxt() (lastPushedMaxt int64, found bool, err error) {
	query, err := utils.CreatePrombQuery(c.Mint, c.Maxt, []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, c.ProgressMetricName),
		labels.MustNewMatcher(labels.MatchEqual, utils.LabelJob, c.JobName),
	})
	if err != nil {
		return -1, false, fmt.Errorf("fetch-last-pushed-maxt create promb query: %w", err)
	}
	readClient, err := utils.NewClient("reader-last-maxt-pushed", c.ProgressClientConfig, c.HTTPConfig)
	if err != nil {
		return -1, false, fmt.Errorf("create fetch-last-pushed-maxt reader: %w", err)
	}
	result, _, _, err := readClient.Read(context.Background(), query, "")
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

func (p *Plan) DecrementSlabCount() {
	atomic.AddInt64(&p.slabCounts, -1)
}

// ShouldProceed reports whether the fetching process should proceeds further. If any time-range is left to be
// fetched from the provided time-range, it returns true, else false.
func (p *Plan) ShouldProceed() bool {
	return p.nextMint < p.config.Maxt
}

// update updates the details of the planner that are dependent on previous fetch stats.
func (p *Plan) update(numBytes int) {
	atomic.StoreInt64(&p.lastNumBytes, int64(numBytes))
}

func (p *Plan) LastMemoryFootprint() int64 {
	return atomic.LoadInt64(&p.lastNumBytes)
}

// NextSlab returns a new slab after allocating the time-range for fetch.
func (p *Plan) NextSlab() (reference *Slab, err error) {
	timeDelta, err := p.config.determineTimeDelta(atomic.LoadInt64(&p.lastNumBytes), p.config.SlabSizeLimitBytes, p.lastTimeRangeDelta)
	if err != nil {
		return nil, fmt.Errorf("determine time delta: %w", err)
	}
	mint := p.nextMint
	maxt := mint + timeDelta
	if maxt > p.config.Maxt {
		maxt = p.config.Maxt
	}
	p.nextMint = maxt
	p.lastTimeRangeDelta = timeDelta
	bRef, err := p.createSlab(mint, maxt)
	if err != nil {
		return nil, fmt.Errorf("create-slab: %w", err)
	}
	return bRef, nil
}

// createSlab creates a new slab and returns reference to the slab for faster write and read operations.
func (p *Plan) createSlab(mint, maxt int64) (ref *Slab, err error) {
	if err = p.validateT(mint, maxt); err != nil {
		return nil, fmt.Errorf("create-slab: %w", err)
	}
	id := atomic.AddInt64(&p.slabCounts, 1)
	timeRangeInMinutes := (maxt - mint) / minute
	percent := float64(maxt-p.config.Mint) * 100 / float64(p.config.Maxt-p.config.Mint)
	if percent > 100 {
		percent = 100
	}
	baseDescription := fmt.Sprintf("progress: %.2f%% | slab-%d time-range: %d mins | start: %d | end: %d", percent, id, timeRangeInMinutes, mint/second, maxt/second)
	if p.config.HumanReadableTime {
		baseDescription = fmt.Sprintf("progress: %.2f%% | slab-%d time-range: %d mins | start: %s | end: %s", percent, id, timeRangeInMinutes, timestamp.Time(mint), timestamp.Time(maxt))
	}
	ref = slabPool.Get().(*Slab)
	ref.id = id
	ref.pbar = progressbar.NewOptions(
		numStepsWriter,
		progressbar.OptionOnCompletion(func() {
			_, _ = fmt.Fprint(os.Stdout, "\n")
		}),
	)
	ref.pbarDescriptionPrefix = baseDescription
	ref.numStores = p.config.NumStores
	if cap(ref.stores) < p.config.NumStores {
		// This is expect to allocate only for the first slab, then the allocated space is reused.
		// On the edge case, we allocate again, which was the previous behaviour.
		ref.stores = make([]store, p.config.NumStores)
	} else {
		ref.stores = ref.stores[:p.config.NumStores]
	}
	ref.mint = mint
	ref.maxt = maxt
	ref.pbarMux = p.pbarMux
	ref.plan = p

	ref.initStores()
	if p.Quiet {
		ref.pbar = nil
		ref.pbarDescriptionPrefix = ""
	}
	if p.TestCheckFunc != nil {
		// This runs only during integration tests. It enables the tests to access and test the internal
		// state of the planner.
		p.TestCheckFunc()
	}
	return
}

func (p *Plan) validateT(mint, maxt int64) error {
	switch {
	case p.config.Mint > mint || p.config.Maxt < mint:
		return fmt.Errorf("invalid mint: %d: global-mint: %d and global-maxt: %d", mint, p.config.Mint, p.config.Maxt)
	case p.config.Mint > maxt || p.config.Maxt < maxt:
		return fmt.Errorf("invalid maxt: %d: global-mint: %d and global-maxt: %d", mint, p.config.Mint, p.config.Maxt)
	case mint > maxt:
		return fmt.Errorf("mint cannot be greater than maxt: mint: %d and maxt: %d", mint, maxt)
	}
	return nil
}

func (c *Config) determineTimeDelta(numBytes, limit int64, prevTimeDelta int64) (int64, error) {
	errOnBelowMinute := func(delta int64) (int64, error) {
		if delta/minute < 1 {
			// This results in an infinite loop, if not errored out. Occurs when the max number of bytes is so low
			// that even the size of 1 minute slab of migration exceeds the bytes limit, resulting in further (exponential)
			// decrease of time-range.
			//
			// Hence, stop migration and report in logs.
			return delta, fmt.Errorf("slab time-range less than a minute. Please increase slab/read limitations")
		}
		return delta, nil
	}
	switch {
	case numBytes <= limit/2:
		// deltaIncreaseRegion.
		// We increase the time-range linearly for the next fetch if the current time-range fetch resulted in size that is
		// less than half the max read size. This continues till we reach the maximum time-range delta.
		return errOnBelowMinute(c.clampTimeDelta(prevTimeDelta + c.LaIncrement.Milliseconds()))
	case numBytes > limit:
		// Decrease size the time exponentially so that bytes size can be controlled (preventing OOM).
		log.Info("msg", fmt.Sprintf("decreasing time-range delta to %d minute(s) since size beyond permittable limits", prevTimeDelta/(2*minute)))
		return errOnBelowMinute(prevTimeDelta / 2)
	}
	// Here, the numBytes is between the max increment-time size limit (i.e., limit/2) and the max read limit
	// (i.e., increment-time size limit < numBytes <= max read limit). This region is an ideal case of
	// balance between migration speed and memory utilization.
	//
	// Example: If the limit is 500MB, then the max increment-time size limit will be 250MB. This means that till the numBytes is below
	// 250MB, the time-range for next fetch will continue to increase by 1 minute (on the previous fetch time-range). However,
	// the moment any slab comes between 250MB and 500MB, we stop to increment the time-range delta further. This helps
	// keeping the migration tool in safe memory limits.
	return prevTimeDelta, nil
}

func (c *Config) clampTimeDelta(t int64) int64 {
	if t > c.MaxReadDuration.Milliseconds() {
		log.Info("msg", "Exceeded 'max-read-duration', setting back to 'max-read-duration' value", "max-read-duration", c.MaxReadDuration.String())
		return c.MaxReadDuration.Milliseconds()
	}
	return t
}
