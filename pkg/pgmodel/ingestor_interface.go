package pgmodel

import "github.com/prometheus/prometheus/prompb"

// DBInserter is responsible for ingesting the TimeSeries protobuf structs and
// storing them in the database.
type DBInserter interface {
	// Ingest takes an array of TimeSeries and attepts to store it into the database.
	// Returns the number of metrics ingested and any error encountered before finishing.
	Ingest([]prompb.TimeSeries) (uint64, error)
}
