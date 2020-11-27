package planner

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/schollz/progressbar/v3"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
)

// Block represents an in-memory storage for data that is fetched by the reader.
type Block struct {
	id                   int64
	mint                 int64
	maxt                 int64
	done                 bool
	percent              float64
	pbarInitDetails      string
	pbar                 *progressbar.ProgressBar
	Timeseries           []*prompb.TimeSeries
	numBytesCompressed   int
	numBytesUncompressed int
}

// Fetch starts fetching the samples from remote read storage based on the matchers.
func (b *Block) Fetch(context context.Context, client *utils.Client, mint, maxt int64, matchers []*labels.Matcher) (*prompb.QueryResult, int, int, error) {
	readRequest, err := utils.CreatePrombQuery(mint, maxt, matchers)
	if err != nil {
		return nil, -1, -1, fmt.Errorf("create promb query: %w", err)
	}
	result, bytesCompressed, bytesUncompressed, err := client.Read(context, readRequest)
	if err != nil {
		return nil, -1, -1, fmt.Errorf("executing client-read: %w", err)
	}
	return result, bytesCompressed, bytesUncompressed, nil
}

func (b *Block) SetData(timeseries []*prompb.TimeSeries) {
	b.Timeseries = timeseries
}

func (b *Block) SetDescription(description string, proceed int) {
	_ = b.pbar.Add(proceed)
	b.pbar.Describe(fmt.Sprintf("progress: %.3f%% | %s | %s", b.percent, b.pbarInitDetails, description))
}

// SetBytes sets the number of bytes of the a compressed block.
func (b *Block) SetBytesCompressed(num int) {
	b.numBytesCompressed = num
}

// SetBytesUncompressed sets the number of bytes of an uncompressed block.
func (b *Block) SetBytesUncompressed(num int) {
	b.numBytesUncompressed = num
}

// BytesCompressed returns the number of bytes of the compressed block.
func (b *Block) BytesCompressed() int {
	return b.numBytesCompressed
}

// BytesUncompressed returns the number of bytes of the compressed block.
func (b *Block) BytesUncompressed() int {
	return b.numBytesUncompressed
}

// Done updates the text and sets the spinner to done.
func (b *Block) Done() error {
	b.SetDescription(
		fmt.Sprintf("pushed %.2f MB. Memory footprint: %.2f MB.", float64(b.numBytesCompressed)/float64(utils.Megabyte), float64(b.numBytesUncompressed)/float64(utils.Megabyte)),
		1,
	)
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
