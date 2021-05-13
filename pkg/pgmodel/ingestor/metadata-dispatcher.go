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

type metadataDispatcher struct {
	conn pgxconn.PgxConn
}

func newMetadataDispatcher(conn pgxconn.PgxConn) *metadataDispatcher {
	return &metadataDispatcher{conn}
}

// CompleteMetricCreation to implement the Dispatcher interface.
func (m *metadataDispatcher) CompleteMetricCreation() error { return nil }

// Close to implement the Dispatcher interface.
func (m *metadataDispatcher) Close() {}

func (m *metadataDispatcher) Insert(data interface{}) (uint64, error) {
	rows, ok := data.([]model.Metadata)
	if !ok {
		panic("invalid data received")
	}
	totalRows := uint64(len(rows))
	// Ingest metadata.
	insertedRows, err := insertMetadataWithBackoff(m.conn, rows)
	if err != nil {
		return insertedRows, err
	}
	if totalRows != insertedRows {
		return insertedRows, fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return insertedRows, nil
}

func insertMetadataWithBackoff(conn pgxconn.PgxConn, data []model.Metadata) (uint64, error) {
	// Retry with backoff.
	time.Sleep(backoffDuration)
	return insertMetadata(conn, data)
}

func insertMetadata(conn pgxconn.PgxConn, reqs []model.Metadata) (insertedRows uint64, err error) {
	batch := conn.NewBatch()
	for _, m := range reqs {
		batch.Queue("SELECT "+schema.Prom+".insert_metric_metadata($1::TIMESTAMPTZ, $2, $3, $4, $5)", time.Now().Local(), m.MetricFamily, m.Type, m.Unit, m.Help)
	}
	// todo: add stats related to ingestion. (see insertSamples for more info)
	results, err := conn.SendBatch(context.Background(), batch)
	if err != nil {
		return 0, fmt.Errorf("send metadata batch: %w", err)
	}
	defer results.Close()
	for range reqs {
		var rows int64
		if err := results.QueryRow().Scan(&rows); err != nil {
			return insertedRows, fmt.Errorf("insert metadata: %w", err)
		}
		insertedRows += uint64(rows)
	}
	return insertedRows, nil
}
