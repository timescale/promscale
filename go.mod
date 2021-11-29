module github.com/timescale/promscale

go 1.15

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/NYTimes/gziphandler v1.1.1
	github.com/armon/go-metrics v0.3.9 // indirect
	github.com/blang/semver/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/containerd/cgroups v1.0.2
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/felixge/fgprof v0.9.1
	github.com/go-kit/log v0.2.0
	github.com/go-resty/resty/v2 v2.6.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.0.0
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-plugin v1.4.3
	github.com/inhies/go-bytesize v0.0.0-20201103132853-d0aed0d254f8
	github.com/jackc/pgconn v1.10.1
	github.com/jackc/pgerrcode v0.0.0-20190803225404-afa3381909a6
	github.com/jackc/pgproto3/v2 v2.2.0
	github.com/jackc/pgtype v1.9.1
	github.com/jackc/pgx/v4 v4.14.0
	github.com/jaegertracing/jaeger v1.28.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger v0.40.0
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pbnjay/memory v0.0.0-20201129165224-b12e5d931931
	github.com/peterbourgon/ff/v3 v3.1.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.32.1
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/prometheus/prometheus v1.8.2-0.20210605142932-7bc11dcb0664
	github.com/schollz/progressbar/v3 v3.8.3
	github.com/sergi/go-diff v1.2.0
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546
	github.com/stretchr/testify v1.7.0
	github.com/testcontainers/testcontainers-go v0.10.1-0.20210318151656-2bbeb1e04514
	github.com/thanos-io/thanos v0.20.1
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	go.opentelemetry.io/collector/model v0.40.0
	go.uber.org/automaxprocs v1.4.0
	go.uber.org/goleak v1.1.12
	google.golang.org/grpc v1.42.0
	gopkg.in/yaml.v2 v2.4.0
)

// Make sure Prometheus version is pinned as Prometheus semver does not include Go APIs.
replace github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20211026060625-c2d1c858577c
