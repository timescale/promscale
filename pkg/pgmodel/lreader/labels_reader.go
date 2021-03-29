// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package lreader

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/pgmodel/model/pgsafetype"
	"sort"
	"unsafe"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

const (
	getLabelNamesSQL  = "SELECT distinct key from " + schema.Catalog + ".label"
	getLabelValuesSQL = "SELECT value from " + schema.Catalog + ".label WHERE key = $1"
	getLabelsSQL      = "SELECT (labels_info($1::int[])).*"
)

// LabelsReader defines the methods for accessing labels data
type LabelsReader interface {
	// LabelNames returns all the distinct label names in the system.
	LabelNames() ([]string, error)
	// LabelValues returns all the distinct values for a given label name.
	LabelValues(labelName string) ([]string, error)
	// PrompbLabelsForIds returns protobuf representation of the label names
	// and values for supplied IDs.
	PrompbLabelsForIds(ids []int64) (lls []prompb.Label, err error)
	// LabelsForIds returns label names and values for the supplied IDs.
	LabelsForIds(ids []int64) (lls labels.Labels, err error)
}

func NewLabelsReader(conn pgxconn.PgxConn, labels cache.LabelsCache) LabelsReader {
	return &labelsReader{conn: conn, labels: labels}
}

type labelsReader struct {
	conn   pgxconn.PgxConn
	labels cache.LabelsCache
}

// LabelValues implements the LabelsReader interface. It returns all distinct values
// for a specified label name.
func (lr *labelsReader) LabelValues(labelName string) ([]string, error) {
	rows, err := lr.conn.Query(context.Background(), getLabelValuesSQL, labelName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelValues := make([]string, 0)

	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}

		labelValues = append(labelValues, value)
	}

	sort.Strings(labelValues)
	return labelValues, nil
}

// LabelNames implements the LabelReader interface. It returns all distinct
// label names available in the database.
func (lr *labelsReader) LabelNames() ([]string, error) {
	rows, err := lr.conn.Query(context.Background(), getLabelNamesSQL)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	labelNames := make([]string, 0)

	for rows.Next() {
		var labelName string
		if err := rows.Scan(&labelName); err != nil {
			return nil, err
		}

		labelNames = append(labelNames, labelName)
	}

	sort.Strings(labelNames)
	return labelNames, nil
}

// PrompbLabelsForIds returns protobuf representation of the label sets for
// the provided label ids
func (lr *labelsReader) PrompbLabelsForIds(ids []int64) (lls []prompb.Label, err error) {
	ll, err := lr.LabelsForIds(ids)
	if err != nil {
		return
	}
	lls = make([]prompb.Label, len(ll))
	for i := range ll {
		lls[i] = prompb.Label{Name: ll[i].Name, Value: ll[i].Value}
	}
	return
}

// LabelsForIds returns label names and values for the supplied IDs.
func (lr *labelsReader) LabelsForIds(ids []int64) (lls labels.Labels, err error) {
	keys := make([]interface{}, len(ids))
	values := make([]interface{}, len(ids))
	for i := range ids {
		keys[i] = ids[i]
	}
	numHits := lr.labels.GetValues(keys, values)

	if numHits < len(ids) {
		var numFetches int
		numFetches, err = lr.fetchMissingLabels(keys[numHits:], ids[numHits:], values[numHits:])
		if err != nil {
			return
		}
		values = values[:numHits+numFetches]
	}

	lls = make([]labels.Label, 0, len(values))
	for i := range values {
		lls = append(lls, values[i].(labels.Label))
	}

	return
}

// fetchMissingLabels imports the missing label IDs from the database into the
// internal cache. It also modifies the newLabels slice to include the missing
// values.
func (lr *labelsReader) fetchMissingLabels(misses []interface{}, missedIds []int64, newLabels []interface{}) (numNewLabels int, err error) {
	for i := range misses {
		missedIds[i] = misses[i].(int64)
	}
	rows, err := lr.conn.Query(context.Background(), getLabelsSQL, missedIds)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ids  []int64
			keys = new(pgsafetype.TextArray)
			vals = new(pgsafetype.TextArray)
		)
		err = rows.Scan(&ids, keys, vals)
		if err != nil {
			return 0, err
		}
		if len(ids) != len(keys.Elements) {
			return 0, fmt.Errorf("query returned a mismatch in ids and keys: %d, %d", len(ids), len(keys.Elements))
		}
		if len(keys.Elements) != len(vals.Elements) {
			return 0, fmt.Errorf("query returned a mismatch in timestamps and values: %d, %d", len(keys.Elements), len(vals.Elements))
		}
		if len(keys.Elements) > len(misses) {
			return 0, fmt.Errorf("query returned wrong number of labels: %d, %d", len(misses), len(keys.Elements))
		}

		numNewLabels = len(keys.Elements)
		misses = misses[:len(keys.Elements)]
		newLabels = newLabels[:len(keys.Elements)]
		sizes := make([]uint64, numNewLabels)
		keysArr := keys.Get().([]string)
		valsArr := vals.Get().([]string)
		for i := range newLabels {
			misses[i] = ids[i]
			newLabels[i] = labels.Label{Name: keysArr[i], Value: valsArr[i]}
			sizes[i] = uint64(8 + int(unsafe.Sizeof(labels.Label{})) + len(keysArr[i]) + len(valsArr[i])) // #nosec
		}

		numInserted := lr.labels.InsertBatch(misses, newLabels, sizes)
		if numInserted < len(misses) {
			log.Warn("msg", "labels cache starving, may need to increase size")
		}
	}
	return numNewLabels, nil
}
