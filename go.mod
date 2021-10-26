module github.com/timescale/promscale

go 1.15

require (
	github.com/Microsoft/go-winio v0.5.1 // indirect
	github.com/Microsoft/hcsshim v0.9.0 // indirect
	github.com/NYTimes/gziphandler v1.1.1
	github.com/alecthomas/units v0.0.0-20210927113745-59d0afb8317a // indirect
	github.com/aws/aws-sdk-go v1.41.10 // indirect
	github.com/blang/semver/v4 v4.0.0
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/cilium/ebpf v0.7.0 // indirect
	github.com/containerd/cgroups v1.0.2
	github.com/docker/docker v20.10.10+incompatible // indirect
	github.com/docker/go-connections v0.4.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/fatih/color v1.13.0 // indirect
	github.com/felixge/fgprof v0.9.1
	github.com/go-kit/kit v0.12.0
	github.com/go-kit/log v0.2.0
	github.com/go-resty/resty/v2 v2.6.0 // indirect
	github.com/godbus/dbus/v5 v5.0.5 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/golang/snappy v0.0.4
	github.com/google/pprof v0.0.0-20211008130755-947d60d73cc0 // indirect
	github.com/hashicorp/go-hclog v1.0.0
	github.com/hashicorp/go-plugin v1.4.3
	github.com/hashicorp/yamux v0.0.0-20210826001029-26ff87cf9493 // indirect
	github.com/inhies/go-bytesize v0.0.0-20210819104631-275770b98743
	github.com/jackc/pgconn v1.10.0
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451
	github.com/jackc/pgproto3/v2 v2.1.1
	github.com/jackc/pgtype v1.8.1
	github.com/jackc/pgx/v4 v4.13.0
	github.com/jackc/puddle v1.1.4 // indirect
	github.com/jaegertracing/jaeger v1.27.0
	github.com/linode/linodego v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger v0.37.1
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/peterbourgon/ff/v3 v3.1.2
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/prometheus/common v0.32.1
	github.com/prometheus/prometheus v1.8.2-0.20210914090109-37468d88dce8
	github.com/schollz/progressbar/v3 v3.8.3
	github.com/sergi/go-diff v1.2.0
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546
	github.com/stretchr/testify v1.7.0
	github.com/testcontainers/testcontainers-go v0.11.1
	github.com/thanos-io/thanos v0.23.1
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	go.opentelemetry.io/collector/model v0.37.0
	go.uber.org/goleak v1.1.12
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211020060615-d418f374d309 // indirect
	golang.org/x/oauth2 v0.0.0-20211005180243-6b3c2da341f1 // indirect
	golang.org/x/sys v0.0.0-20211025201205-69cdffdb9359 // indirect
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211 // indirect
	google.golang.org/genproto v0.0.0-20211021150943-2b146023228c // indirect
	google.golang.org/grpc v1.41.0
	gopkg.in/yaml.v2 v2.4.0
)

// Make sure Prometheus version is pinned as Prometheus semver does not include Go APIs.
replace github.com/prometheus/prometheus => github.com/prometheus/prometheus v1.8.2-0.20210923143046-587cb950930e
