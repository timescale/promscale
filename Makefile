.PHONY: build
build:
	go build -o dist/promscale ./cmd/promscale

test:
	go test -v -race ./...

e2e-test:
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false -use-timescaledb=false
	go test -v ./pkg/tests/end_to_end_tests/ -use-timescale2
	go test -v ./pkg/tests/end_to_end_tests/ -use-extension=false -use-timescale2
	go test -v ./pkg/tests/end_to_end_tests/ -use-multinode


go-fmt:
	gofmt -d .

go-lint:
	golangci-lint run --skip-dirs=pkg/promql --skip-dirs=pkg/promb

all: build test e2e-test go-fmt go-lint