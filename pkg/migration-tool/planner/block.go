package planner

import (
	"fmt"
	"os"
	"time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

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
		return fmt.Errorf("invalid mint: %d: global-mint: %d and global-maxt: %d", mint, p.Mint, p.Maxt)
	case p.Mint > maxt || p.Maxt < maxt:
		return fmt.Errorf("invalid maxt: %d: global-mint: %d and global-maxt: %d", mint, p.Mint, p.Maxt)
	case mint > maxt:
		return fmt.Errorf("mint cannot be greater than maxt: mint: %d and maxt: %d", mint, maxt)
	}
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

// SetBytes sets the number of bytes of the block.
func (b *Block) SetBytes(num int) {
	b.numBytes = num
}

// Bytes returns the number of bytes of the block.
func (b *Block) Bytes() int {
	return b.numBytes
}

// Done updates the text and sets the spinner to done.
func (b *Block) Done() error {
	b.SetDescription(fmt.Sprintf("pushed %.2f MB", float64(b.numBytes)/float64(utils.Megabyte)), 1)
	b.done = true
	if err := b.pbar.Finish(); err != nil {
		return fmt.Errorf("finish block-lifecycle: %w", err)
	}
	return nil
}

// Mint returns the mint of the block.
func (b *Block) Mint() int64 {
	return b.mint
}

// Maxt returns the maxt of the block.
func (b *Block) Maxt() int64 {
	return b.maxt
}
