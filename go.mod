module github.com/timescale/promscale

go 1.15

require (
	github.com/NYTimes/gziphandler v1.1.1
	github.com/blang/semver/v4 v4.0.0
	github.com/containerd/cgroups v1.0.3
	github.com/dekobon/distro-detect v0.0.0-20201122001546-5f5b9c724b9d
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.1.0
	github.com/felixge/fgprof v0.9.2
	github.com/go-kit/log v0.2.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/grafana/regexp v0.0.0-20220304095617-2e8d9baf4ac2
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/hashicorp/go-hclog v1.2.0
	github.com/jackc/pgconn v1.11.0
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa
	github.com/jackc/pgproto3/v2 v2.3.0
	github.com/jackc/pgtype v1.11.0
	github.com/jackc/pgx/v4 v4.15.0
	github.com/jaegertracing/jaeger v1.33.0
	github.com/oklog/run v1.1.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger v0.49.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/peterbourgon/ff/v3 v3.1.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.34.0
	github.com/prometheus/prometheus v1.8.2-0.20220117154355-4855a0c067e2
	github.com/sergi/go-diff v1.2.0
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546
	github.com/stretchr/testify v1.7.1
	github.com/testcontainers/testcontainers-go v0.13.0
	github.com/thanos-io/thanos v0.25.2
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	go.opentelemetry.io/collector/model v0.49.0
	go.opentelemetry.io/collector/pdata v0.49.0
	go.opentelemetry.io/collector/semconv v0.52.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.32.0
	go.opentelemetry.io/otel v1.7.0
	go.opentelemetry.io/otel/exporters/jaeger v1.6.3
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.6.3
	go.opentelemetry.io/otel/sdk v1.6.3
	go.opentelemetry.io/otel/trace v1.7.0
	go.uber.org/automaxprocs v1.5.1
	go.uber.org/goleak v1.1.12
	golang.org/x/sys v0.0.0-20220422013727-9388b58f7150
	google.golang.org/genproto v0.0.0-20220217155828-d576998c0009 // indirect
	google.golang.org/grpc v1.46.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/yaml.v2 v2.4.0
)

// Make sure Prometheus version is pinned as Prometheus semver does not include Go APIs.
replace github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20220117154355-4855a0c067e2
