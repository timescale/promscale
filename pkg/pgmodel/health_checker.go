// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"context"
	"github.com/timescale/promscale/pkg/pgxconn"
)

//HealthChecker allows checking for proper operations.
type HealthChecker interface {
	HealthCheck() error
}

func NewHealthChecker(conn pgxconn.PgxConn) HealthChecker {
	return &pgxHealthChecker{
		conn: conn,
	}
}

type pgxHealthChecker struct {
	conn pgxconn.PgxConn
}

// HealthCheck implements the HealthChecker interface.
func (hc *pgxHealthChecker) HealthCheck() error {
	rows, err := hc.conn.Query(context.Background(), "SELECT")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}
