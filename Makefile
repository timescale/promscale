GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)

.PHONY: build
build: generate
	go build -ldflags "-X 'github.com/timescale/promscale/pkg/version.Branch=${GIT_BRANCH}' -X 'github.com/timescale/promscale/pkg/version.CommitHash=${GIT_COMMIT}'" -o dist/promscale ./cmd/promscale

.PHONY: test
test: generate
	go test -v -race ./... -timeout 40m

# traces-dataset.sz is used by ./pkg/tests/end_to_end_tests/ingest_trace_test.go
pkg/tests/testdata/traces-dataset.sz:
	wget https://github.com/timescale/promscale-test-data/raw/main/traces-dataset.sz -O ./pkg/tests/testdata/traces-dataset.sz

.PHONY: e2e-test
e2e-test: pkg/tests/testdata/traces-dataset.sz generate
	go test -v ./pkg/tests/end_to_end_tests/
	go test -v ./pkg/tests/end_to_end_tests/ -use-timescaledb=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-multinode

.PHONY: go-fmt
go-fmt:
	go fmt ./...

.PHONY: go-lint
go-lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: check-alerts
check-alerts:
	# If you don't have promtool, install it with
	# go install -a github.com/prometheus/prometheus/cmd/promtool@latest
	promtool check rules docs/mixin/alerts/alerts.yaml

.PHONY: all
all: build test e2e-test go-fmt go-lint
