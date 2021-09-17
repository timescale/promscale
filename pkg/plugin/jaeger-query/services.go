package jaeger_query

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/timescale/promscale/pkg/pgxconn"
)

const getServices = "select array_agg(value) from _ps_trace.tag where key='service.name' group by value order by value"

func services(ctx context.Context, conn pgxconn.PgxConn) ([]string, error) {
	services := make([]string, 0)
	if err := conn.QueryRow(ctx, getServices).Scan(&services); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			services = append(services, "mock_service") // only for debug
			return services, nil
		}
		return nil, fmt.Errorf("fetching services: %w", err)
	}
	return services, nil
}
