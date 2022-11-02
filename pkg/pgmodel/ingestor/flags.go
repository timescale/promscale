// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"flag"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/ingestor/trace"
)

type Config struct {
	MetricsAsyncAcks       bool
	TracesAsyncAcks        bool
	DisableEpochSync       bool
	IgnoreCompressedChunks bool
	TracesBatchTimeout     time.Duration
	TracesMaxBatchSize     int
	TracesBatchWorkers     int
}

func ParseFlags(fs *flag.FlagSet, cfg *Config) {
	fs.BoolVar(&cfg.IgnoreCompressedChunks, "metrics.ignore-samples-written-to-compressed-chunks", false, "Ignore/drop samples that are being written to compressed chunks. "+
		"Setting this to false allows Promscale to ingest older data by decompressing chunks that were earlier compressed. "+
		"However, setting this to true will save your resources that may be required during decompression. ")
	fs.BoolVar(&cfg.MetricsAsyncAcks, "metrics.async-acks", false, "Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of metric data in the database. This increases throughput at the cost of a small chance of data loss.")
	fs.BoolVar(&cfg.TracesAsyncAcks, "tracing.async-acks", true, "Acknowledge asynchronous inserts. If this is true, the inserter will not wait after insertion of traces data in the database. This increases throughput at the cost of a small chance of data loss.")
	fs.IntVar(&cfg.TracesMaxBatchSize, "tracing.max-batch-size", trace.DefaultBatchSize, "Maximum size of trace batch that is written to DB")
	fs.DurationVar(&cfg.TracesBatchTimeout, "tracing.batch-timeout", trace.DefaultBatchTimeout, "Timeout after new trace batch is created")
	fs.IntVar(&cfg.TracesBatchWorkers, "tracing.batch-workers", trace.DefaultBatchWorkers, "Number of workers responsible for creating trace batches. Defaults to number of CPUs.")
}

func Validate(cfg *Config) error {
	return nil
}
