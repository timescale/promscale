// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"time"

	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const backoffDuration = time.Millisecond * 500

type metadataRequest struct {
	batch []model.Metadata
	errCh chan<- error
}

func handleMetadataRequest(conn pgxconn.PgxConn, batch []metadataRequest) {
	if err := insertMetadata(conn, batch); err != nil {
		if err = insertWithBackoff(conn, batch); err != nil {
			for i := range batch {
				batch[i].errCh <- err
			}
		}
	}
	for i := range batch {
		close(batch[i].errCh)
	}
}

func insertWithBackoff(conn pgxconn.PgxConn, batch []metadataRequest) error {
	// Retry with backoff.
	time.Sleep(backoffDuration)
	return insertMetadata(conn, batch)
}

func insertMetadata(conn pgxconn.PgxConn, reqs []metadataRequest) (err error) {
	var (
		totalRows uint64
		batch     = conn.NewBatch()
	)
	for _, req := range reqs {
		totalRows += uint64(len(req.batch))
		for _, m := range req.batch {
			data := m.MetricMetadata
			batch.Queue("SELECT "+schema.Prom+".insert_metric_metadata($1::TIMESTAMPTZ, $2, $3, $4, $5)", time.Now().Local(), data.MetricFamilyName, data.Type.String(), data.Unit, data.Help)
		}
	}
	// todo: add stats related to ingestion. (see insertSamples for more info)
	results, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return fmt.Errorf("send metadata batch: %w", err)
	}
	defer results.Close()
	var insertedRows uint64
	for i := range reqs {
		for range reqs[i].batch {
			var rows int64
			if err := results.QueryRow().Scan(&rows); err != nil {
				return fmt.Errorf("insert metadata: %w", err)
			}
			insertedRows += uint64(rows)
		}
	}
	if totalRows != insertedRows {
		return fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return nil
}
