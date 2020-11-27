module github.com/timescale/promscale

//TODO remove after repo rename
replace github.com/timescale/promscale => ./

go 1.14

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/blang/semver/v4 v4.0.0
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/go-kit/kit v0.10.0
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/golang/snappy v0.0.2
	github.com/jackc/pgconn v1.6.3
	github.com/jackc/pgerrcode v0.0.0-20190803225404-afa3381909a6
	github.com/jackc/pgproto3/v2 v2.0.2
	github.com/jackc/pgtype v1.4.2
	github.com/jackc/pgx/v4 v4.8.0
	github.com/jamiealquiza/envy v1.1.0
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.8.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.13.0
	github.com/prometheus/prometheus v1.8.2-0.20200907175821-8219b442c864
	github.com/schollz/progressbar/v3 v3.7.0
	github.com/spf13/cobra v0.0.7 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/testcontainers/testcontainers-go v0.5.1
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	go.uber.org/atomic v1.6.0
	golang.org/x/net v0.0.0-20200904194848-62affa334b73 // indirect
	golang.org/x/tools v0.0.0-20200908211811-12e1bf57a112 // indirect
	google.golang.org/genproto v0.0.0-20200904004341-0bd0a958aa1d
	google.golang.org/grpc v1.32.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)
