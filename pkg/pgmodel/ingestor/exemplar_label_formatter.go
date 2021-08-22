package ingestor

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	getExemplarLabelPositions = "SELECT * FROM " + schema.Catalog + ".get_new_pos_for_key($1::TEXT, $2::TEXT[], true)"
)

type ExemplarVisitor interface {
	VisitExemplar(func(s *model.PromExemplars) error) error
}

type ExemplarLabelFormatter struct {
	conn                pgxconn.PgxConn
	exemplarKeyPosCache cache.PositionCache
}

func NewExamplarLabelFormatter(conn pgxconn.PgxConn, exemplarKeyPosCache cache.PositionCache) *ExemplarLabelFormatter {
	return &ExemplarLabelFormatter{conn, exemplarKeyPosCache}
}

type positionPending struct {
	exemplarRef *model.PromExemplars
	metricName  string
	labelKeys   []string
}

func (t *ExemplarLabelFormatter) orderExemplarLabelValues(ev ExemplarVisitor) error {
	var (
		batch          pgxconn.PgxBatch
		pendingIndexes []positionPending
	)

	err := ev.VisitExemplar(func(row *model.PromExemplars) error {
		labelKeyIndex, entryExists := t.exemplarKeyPosCache.GetLabelPositions(row.Series().MetricName())
		if entryExists {
			//make sure all positions exist
			if positionExists := row.OrderExemplarLabels(labelKeyIndex); positionExists {
				return nil
			}
		}
		if unorderedLabelKeys := row.AllExemplarLabelKeys(); len(unorderedLabelKeys) != 0 {
			// Do not fetch exemplar label positions if there are no labels/keys in the given exemplars.
			if batch == nil {
				// Allocate a batch only if required. If the cache does the job, why to waste on allocs.
				batch = t.conn.NewBatch()
			}
			batch.Queue(getExemplarLabelPositions, row.Series().MetricName(), unorderedLabelKeys)
			pendingIndexes = append(pendingIndexes, positionPending{ // One-to-One relation with queries
				exemplarRef: row,
				metricName:  row.Series().MetricName(),
				labelKeys:   unorderedLabelKeys,
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(pendingIndexes) > 0 {
		// There are positions that require to be fetched. Let's fetch them and fill our indexes.
		// pendingIndexes contain the exact array index for rows, where the cache miss were found. Let's
		// use the pendingIndexes to go to those rows and order the labels in exemplars quickly.
		results, err := t.conn.SendBatch(context.Background(), batch)
		if err != nil {
			return fmt.Errorf("sending fetch label key positions batch: %w", err)
		}
		defer results.Close()
		for _, pending := range pendingIndexes {
			var (
				correspondingKeyPositions []int
				labelKeyIndex             = make(map[string]int)
			)
			err := results.QueryRow().Scan(&correspondingKeyPositions)
			if err != nil {
				return fmt.Errorf("fetching label key positions: %w", err)
			}
			metricName := pending.metricName
			labelKeys := pending.labelKeys
			buildKeyIndex(labelKeyIndex, labelKeys, correspondingKeyPositions)
			t.exemplarKeyPosCache.SetorUpdateLabelPositions(metricName, labelKeyIndex)
			exemplarRef := pending.exemplarRef
			if found := exemplarRef.OrderExemplarLabels(labelKeyIndex); !found {
				// Sanity check, since we just filled the missing labels positions.
				return fmt.Errorf("unable to order exemplar labels even after fetching recent positons. " +
					"Please report an issue at https://github.com/timescale/promscale/issues")
			}
		}
	}
	return nil
}

func buildKeyIndex(indexRef map[string]int, keyNames []string, keyPositions []int) {
	for i := range keyNames {
		key := keyNames[i]
		pos := keyPositions[i]
		indexRef[key] = pos
	}
}
