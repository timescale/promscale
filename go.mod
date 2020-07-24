module github.com/timescale/timescale-prometheus

go 1.14

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/OneOfOne/xxhash v1.2.5 // indirect
	github.com/allegro/bigcache v1.2.1
	github.com/blang/semver/v4 v4.0.0
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.1
	github.com/grpc-ecosystem/grpc-gateway v1.14.4
	github.com/jackc/pgconn v1.3.2
	github.com/jackc/pgerrcode v0.0.0-20190803225404-afa3381909a6
	github.com/jackc/pgproto3/v2 v2.0.1
	github.com/jackc/pgtype v1.2.0
	github.com/jackc/pgx/v4 v4.4.1
	github.com/jamiealquiza/envy v1.1.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.9.1
	github.com/prometheus/prometheus v1.8.2-0.20200507164740-ecee9c8abfd1
	github.com/spf13/cobra v0.0.7 // indirect
	github.com/testcontainers/testcontainers-go v0.5.1
	github.com/uber/jaeger-client-go v2.23.0+incompatible
	google.golang.org/genproto v0.0.0-20200420144010-e5e8543f8aeb
	google.golang.org/grpc v1.29.0
)

replace github.com/jackc/pgconn => github.com/JLockerman/pgconn v1.5.3-0.20200513205926-64cd2ce264ca
