package planner

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

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

// Fetch starts fetching the samples from remote read storage based on the matchers.
func (b *Block) Fetch(context context.Context, client remote.ReadClient, mint, maxt int64, matchers []*labels.Matcher) (*prompb.QueryResult, error) {
	readRequest, err := utils.CreatePrombQuery(mint, maxt, matchers)
	if err != nil {
		return nil, fmt.Errorf("create promb query: %w", err)
	}
	result, err := client.Read(context, readRequest)
	if err != nil {
		return nil, fmt.Errorf("executing client-read: %w", err)
	}
	return result, nil
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
