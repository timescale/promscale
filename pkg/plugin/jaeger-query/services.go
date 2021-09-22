package jaeger_query

import (
	"context"
	"fmt"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getServices = "select array_agg(a.value) from (select value from _ps_trace.tag where key='service.name' and value is not null order by value) a"

func services(ctx context.Context, conn pgxconn.PgxConn) ([]string, error) {
	var pgServices pgtype.TextArray
	if err := conn.QueryRow(ctx, getServices).Scan(&pgServices); err != nil {
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
