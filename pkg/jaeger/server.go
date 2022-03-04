package jaeger

import (
	"context"
	"fmt"
	"net"
	"net/http"

	jaegerQueryApp "github.com/jaegertracing/jaeger/cmd/query/app"
	jaegerQueryService "github.com/jaegertracing/jaeger/cmd/query/app/querysvc"

	"github.com/timescale/promscale/pkg/jaeger/query"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/telemetry"
)

type Server struct {
	server *http.Server
	listen net.Listener
}

func New(conn pgxconn.PgxConn, port string, t telemetry.Engine) (*Server, error) {
	reader, err := query.New(conn, t)
	if err != nil {
		return nil, fmt.Errorf("error creating jaeger-query: %w", err)
	}
	handler := jaegerQueryApp.NewAPIHandler(
		jaegerQueryService.NewQueryService(reader, reader, jaegerQueryService.QueryServiceOptions{}),
	)
	r := jaegerQueryApp.NewRouter()
	handler.RegisterRoutes(r)

	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, fmt.Errorf("error creating listener: %w", err)
	}
	return &Server{
		server: &http.Server{
			Handler: r,
		},
		listen: listener,
	}, nil
}

// Start starts the HTTP server at the given address and blocks until the server stops or errors out.
func (s *Server) Start() error {
	return s.server.Serve(s.listen)
}

func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
