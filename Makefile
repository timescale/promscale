.PHONY: build
build: generate
	go build -o dist/promscale ./cmd/promscale

.PHONY: test
test: generate
	go test -v -race ./... -timeout 40m

# traces-dataset.sz is used by ./pkg/tests/end_to_end_tests/ingest_trace_test.go
pkg/tests/testdata/traces-dataset.sz:
	wget https://github.com/timescale/promscale-test-data/raw/main/traces-dataset.sz -O ./pkg/tests/testdata/traces-dataset.sz

.PHONY: e2e-test
e2e-test: pkg/tests/testdata/traces-dataset.sz generate
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false -use-timescaledb=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-timescale2
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false -use-timescale2
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

.PHONY: all
all: build test e2e-test go-fmt go-lint
