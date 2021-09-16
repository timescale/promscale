package jaeger_query

import (
	"context"
	"fmt"

	"github.com/timescale/promscale/pkg/pgxconn"
)

const getServices = ""

func services(ctx context.Context, conn pgxconn.PgxConn) ([]string, error) {
	var services []string // todo: make this make()
	if err := conn.QueryRow(ctx, getServices).Scan(&services); err != nil {
		return nil, fmt.Errorf("fetching services: %w", err)
	}
	return services, nil
}
