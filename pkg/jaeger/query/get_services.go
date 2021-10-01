// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package query

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getServicesSQL = `
SELECT
 	array_agg(value#>>'{}' ORDER BY value)
FROM
	_ps_trace.tag
WHERE
         key='service.name' and value IS NOT NULL`

func getServices(ctx context.Context, conn pgxconn.PgxConn) ([]string, error) {
	var pgServices pgtype.TextArray
	if err := conn.QueryRow(ctx, getServicesSQL).Scan(&pgServices); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("fetching services: %w", err)
	}
	s, err := textArraytoStringArr(pgServices)
	if err != nil {
		return nil, fmt.Errorf("services: converting text-array-to-string-arr: %w", err)
	}
	return s, nil
}
