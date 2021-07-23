package ingestor

import (
	"context"
	"fmt"
	"github.com/timescale/promscale/pkg/log"

	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func processExemplars(conn pgxconn.PgxConn, metricName string, catalog *exemplarInfo, data []model.Insertable) error {
	if !catalog.seenPreviously {
		// We are seeing the exemplar belonging to this metric first time. It may be the
		// first time of this exemplar in the database. So, let's attempt to create a table
		// if it does not exists.
		var created bool // Trivial. Should we remove the scan?
		err := conn.QueryRow(context.Background(), createExemplarTable, metricName).Scan(&created)
		if err != nil {
			return fmt.Errorf("checking exemplar table creation: %w", err)
		}
	}
	err := orderExemplarLabelValues(conn, catalog, data)
	if err != nil {
		return fmt.Errorf("metric-batcher: ordering exemplar label values: %w", err)
	}
	return nil
}

func orderExemplarLabelValues(conn pgxconn.PgxConn, info *exemplarInfo, data []model.Insertable) error {
	var (
		batch          pgxconn.PgxBatch
		pendingIndexes []int
	)

	for i := range data {
		row, isExemplar := data[i].(model.InsertableExemplar)
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
		if needsFetch && len(row.AllExemplarLabelKeys()) != 0 {
			// Do not fetch exemplar label positions if there are no labels/keys in the given exemplars.
			if batch == nil {
				// Allocate a batch only if required. If the cache does the job, why to waste on allocs.
				batch = conn.NewBatch()
			}
			batch.Queue(getExemplarLabelPositions, row.Series().MetricName(), row.AllExemplarLabelKeys())
			pendingIndexes = append(pendingIndexes, i)
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
		for _, index := range pendingIndexes {
			var (
				metricName    string
				labelKeyIndex map[string]int
			)
			err := results.QueryRow().Scan(&metricName, &labelKeyIndex)
			if err != nil {
				return fmt.Errorf("fetching label key positions: %w", err)
			}
			info.exemplarCache.SetorUpdateLabelPositions(metricName, labelKeyIndex)
			row, ok := data[index].(model.InsertableExemplar)
			if !ok {
				// 'ok' is a sanitary check only. This will always be model.InsertableExemplar, so this is done only to
				// catch any edge case, which is never expected. Hence, we ask the user to report to us, than just
				// throwing errors/panics.
				log.Error("msg", fmt.Sprintf("expecting model.InsertableExemplar, but received %T. Please report to the Promscale team by opening an issue at https://github.com/timescale/promscale", data[index]))
				return fmt.Errorf("unexpected type received: expecting model.InsertableExemplar, but received %T ", data[index])
			}
			if found := row.OrderExemplarLabels(labelKeyIndex); !found {
				// Sanity check, since we just filled the missing labels positions.
				return fmt.Errorf("unable to order exemplar labels even after fetching recent positons. " +
					"Please report an issue at https://github.com/timescale/promscale/issues")
			}
		}
	}
	return nil
}
