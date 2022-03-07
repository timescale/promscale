package jaeger

import (
	"fmt"

	"github.com/gorilla/mux"
	jaegerQueryApp "github.com/jaegertracing/jaeger/cmd/query/app"
	jaegerQueryService "github.com/jaegertracing/jaeger/cmd/query/app/querysvc"

	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

func ExtendQueryAPIs(r *mux.Router, conn pgxconn.PgxConn, t telemetry.Engine) error {
	reader, err := query.New(conn, t)
	if err != nil {
		return fmt.Errorf("error initializing tracing query: %w", err)
	}
	handler := jaegerQueryApp.NewAPIHandler(
		jaegerQueryService.NewQueryService(reader, reader, jaegerQueryService.QueryServiceOptions{}),
	)
	handler.RegisterRoutes(r)
	return nil
}
