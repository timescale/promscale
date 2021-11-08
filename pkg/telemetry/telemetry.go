// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license

package telemetry

import (
	"context"
	"fmt"
	"strings"

	"github.com/timescale/promscale/pkg/pgxconn"
)

type telemetryEngine struct {
	conn pgxconn.PgxConn
}

func (t *telemetryEngine) WriteMetadata() error {
	promscale := promscaleMetadata()
	if err := writeMetadata(t.conn, promscale); err != nil {
		return fmt.Errorf("writing metadata for promscale: %w", err)
	}

	tobs := tobsMetadata()
	if len(tobs) > 0 {
		if err := writeMetadata(t.conn, tobs); err != nil {
			return fmt.Errorf("writing metadata for tobs: %w", err)
		}
	}
	return nil
}

const insertMetadataFormat = "( '%s', '%s' )"

func writeMetadata(conn pgxconn.PgxConn, m Metadata) error {
	var clauses []string
	for key, metadata := range m {
		clauses = append(clauses, fmt.Sprintf(insertMetadataFormat, key, metadata))
	}
	query := "INSERT INTO _timescaledb_catalog.metadata VALUES %s "
	query = fmt.Sprintf(query, strings.Join(clauses, ","))
	_, err := conn.Exec(context.Background(), query)
	if err != nil {
		return fmt.Errorf("error exec metadata insert query: %w", err)
	}
	return nil
}
