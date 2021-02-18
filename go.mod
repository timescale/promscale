module github.com/timescale/promscale

go 1.15

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/blang/semver/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/containerd/containerd v1.4.1 //indirect
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/snappy v0.0.2
	github.com/inhies/go-bytesize v0.0.0-20201103132853-d0aed0d254f8
	github.com/jackc/pgconn v1.6.3
	github.com/jackc/pgerrcode v0.0.0-20190803225404-afa3381909a6
	github.com/jackc/pgproto3/v2 v2.0.2
	github.com/jackc/pgtype v1.4.2
	github.com/jackc/pgx/v4 v4.8.0
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/peterbourgon/ff/v3 v3.0.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.9.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.15.0
	github.com/prometheus/prometheus v1.8.2-0.20210121114440-a7e446cf2d2a
	github.com/schollz/progressbar/v3 v3.7.2
	github.com/sergi/go-diff v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/testcontainers/testcontainers-go v0.5.1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.uber.org/atomic v1.7.0
	go.uber.org/goleak v1.1.10
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/sys v0.0.0-20210122093101-04d7465088b8 // indirect
	google.golang.org/grpc v1.33.2
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

replace github.com/docker/docker => github.com/docker/engine v0.0.0-20190717161051-705d9623b7c1
