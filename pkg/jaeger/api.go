package jaeger

import (
	"github.com/gorilla/mux"
	jaegerQueryApp "github.com/jaegertracing/jaeger/cmd/query/app"
	jaegerQueryService "github.com/jaegertracing/jaeger/cmd/query/app/querysvc"

	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/pgxconn"
)

func ExtendQueryAPIs(r *mux.Router, conn pgxconn.PgxConn) {
	reader := query.New(conn)
	handler := jaegerQueryApp.NewAPIHandler(
		jaegerQueryService.NewQueryService(reader, reader, jaegerQueryService.QueryServiceOptions{}),
	)
	handler.RegisterRoutes(r)
}
