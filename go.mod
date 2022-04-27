module github.com/timescale/promscale

//TODO remove after repo rename
replace github.com/timescale/promscale => ./

go 1.14

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/blang/semver/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/containerd/containerd v1.4.1 //indirect
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/felixge/fgprof v0.9.2
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
	//Note: Prometheus is not actually on 1.8.2, rather Prometheus does not
	//follow the v2 go mod conventions with the /v2 path, so go mod assumes
	//it's on 1.x but the commit is from the v2 branch
	//to update: go get -u github.com/prometheus/prometheus@<commit hash>
	github.com/prometheus/prometheus v1.8.2-0.20210119214810-e4487274853c
	github.com/prometheus/tsdb v0.10.0
	github.com/schollz/progressbar/v3 v3.7.2
	github.com/sergi/go-diff v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/testcontainers/testcontainers-go v0.5.1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.uber.org/atomic v1.7.0
	go.uber.org/goleak v1.1.10
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/docker/docker => github.com/docker/engine v0.0.0-20190717161051-705d9623b7c1
