// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.
package utils

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgtype"
	"github.com/prometheus/common/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	PromSchema       = "prom_api"
	SeriesViewSchema = "prom_series"
	MetricViewSchema = "prom_metric"
	DataSchema       = "prom_data"
	DataSeriesSchema = "prom_data_series"
	InfoSchema       = "prom_info"
	CatalogSchema    = "_prom_catalog"
	ExtSchema        = "_prom_ext"

	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + CatalogSchema + ".get_or_create_metric_table_name($1)"
)

var (
	ErrMissingTableName = fmt.Errorf("missing metric table name")
)

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	SampleInfos     []SamplesInfo
	SampleInfoIndex int
	SampleIndex     int
	MinSeen         int64
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	si := SampleInfoIterator{SampleInfos: make([]SamplesInfo, 0)}
	si.ResetPosition()
	return si
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s SamplesInfo) {
	t.SampleInfos = append(t.SampleInfos, s)
}

//ResetPosition resets the iteration position to the beginning
func (t *SampleInfoIterator) ResetPosition() {
	t.SampleIndex = -1
	t.SampleInfoIndex = 0
	t.MinSeen = math.MaxInt64
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.SampleIndex++
	if t.SampleInfoIndex < len(t.SampleInfos) && t.SampleIndex >= len(t.SampleInfos[t.SampleInfoIndex].Samples) {
		t.SampleInfoIndex++
		t.SampleIndex = 0
	}
	return t.SampleInfoIndex < len(t.SampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() (time.Time, float64, SeriesID) {
	info := t.SampleInfos[t.SampleInfoIndex]
	sample := info.Samples[t.SampleIndex]
	if t.MinSeen > sample.Timestamp {
		t.MinSeen = sample.Timestamp
	}
	return model.Time(sample.Timestamp).Time(), sample.Value, info.SeriesID
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

func MetricTableName(conn pgxconn.PgxConn, metric string) (string, bool, error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return "", true, err
	}

	var tableName string
	var possiblyNew bool
	defer res.Close()
	if !res.Next() {
		return "", true, ErrMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, err
	}

	return tableName, possiblyNew, nil
}

func TimestamptzToMs(t pgtype.Timestamptz) int64 {
	switch t.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return t.Time.UnixNano() / 1e6
	}
}
