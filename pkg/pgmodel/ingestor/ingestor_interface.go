// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"io"
	"time"

	"github.com/timescale/promscale/pkg/prompb"
)

// DBInserter is responsible for ingesting the TimeSeries protobuf structs and
// storing them in the database.
type DBInserter interface {
	// Ingest takes an array of TimeSeries and attepts to store it into the database.
	// Returns the number of metrics ingested and any error encountered before finishing.
	IngestProto([]prompb.TimeSeries, *prompb.WriteRequest) (uint64, error)
	// Ingest takes a Prometheus exporter text format and attepts to parse and store it into the database.
	// It also gets the content type used to distinguish which specific text format is used (Prometheus or OpenMetrics)
	// and also the default scrape time for the entries without specified timestamp.
	// Returns the number of metrics ingested and any error encountered before finishing.
	IngestText(r io.Reader, contentType string, scrapeTime time.Time) (uint64, error)
}
