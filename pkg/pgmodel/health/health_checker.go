// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package health

import (
	"context"

	"github.com/timescale/promscale/pkg/pgxconn"
)

// HealthCheckerFn allows checking for proper db operations.
type HealthCheckerFn func() error

func NewHealthChecker(conn pgxconn.PgxConn) HealthCheckerFn {
	return func() error {
		rows, err := conn.Query(context.Background(), "SELECT")

		if err != nil {
			return err
		}

		rows.Close()
		return nil
	}
}
