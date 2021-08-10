package ingestor

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const (
	createExemplarTable       = "SELECT * FROM " + schema.Catalog + ".create_exemplar_table_if_not_exists($1)"
	getExemplarLabelPositions = "SELECT * FROM " + schema.Catalog + ".get_new_pos_for_key($1::TEXT, $2::TEXT[], true)"
)

func containsExemplars(data []model.Insertable) bool {
	for _, row := range data {
		if row.IsOfType(model.Exemplar) {
			return true
		}
	}
	return false
}

func processExemplars(conn pgxconn.PgxConn, metricName string, catalog *exemplarInfo, data []model.Insertable) error {
	if !catalog.seenPreviously {
		// We are seeing the exemplar belonging to this metric first time. It may be the
		// first time of this exemplar in the database. So, let's attempt to create a table
		// if it does not exists.
		var created bool // Trivial. Should we remove the scan?
		err := conn.QueryRow(context.Background(), createExemplarTable, metricName).Scan(&created)
		if err != nil {
			return fmt.Errorf("checking exemplar table creation for metric %s: %w", metricName, err)
		}
		catalog.seenPreviously = true
	}
	if err := orderExemplarLabelValues(conn, catalog, data); err != nil {
		return fmt.Errorf("metric-batcher: ordering exemplar label values: %w", err)
	}
	return nil
}

type positionPending struct {
	exemplarRef *model.PromExemplars
	metricName  string
	labelKeys   []string
}

func orderExemplarLabelValues(conn pgxconn.PgxConn, info *exemplarInfo, data []model.Insertable) error {
	var (
		batch          pgxconn.PgxBatch
		pendingIndexes []positionPending
	)

	for i := range data {
		row, isExemplar := data[i].(*model.PromExemplars)
		if !isExemplar {
			continue
		}
		labelKeyIndex, entryExists := info.exemplarCache.GetLabelPositions(row.Series().MetricName())
		needsFetch := true
		if entryExists {
			if positionExists := row.OrderExemplarLabels(labelKeyIndex); positionExists {
				needsFetch = false
			}
		}
		if unorderedLabelKeys := row.AllExemplarLabelKeys(); needsFetch && len(unorderedLabelKeys) != 0 {
			// Do not fetch exemplar label positions if there are no labels/keys in the given exemplars.
			if batch == nil {
				// Allocate a batch only if required. If the cache does the job, why to waste on allocs.
				batch = conn.NewBatch()
			}
			batch.Queue(getExemplarLabelPositions, row.Series().MetricName(), unorderedLabelKeys)
			pendingIndexes = append(pendingIndexes, positionPending{ // One-to-One relation with
				exemplarRef: row,
				metricName:  row.Series().MetricName(),
				labelKeys:   unorderedLabelKeys,
			})
		}
	}
	if len(pendingIndexes) > 0 {
		// There are positions that require to be fetched. Let's fetch them and fill our indexes.
		// pendingIndexes contain the exact array index for rows, where the cache miss were found. Let's
		// use the pendingIndexes to go to those rows and order the labels in exemplars quickly.
		results, err := conn.SendBatch(context.Background(), batch)
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
			info.exemplarCache.SetorUpdateLabelPositions(metricName, labelKeyIndex)
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
