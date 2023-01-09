GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
LOCAL_DOCKER_BASE := local/dev_promscale_extension
GHCR_DOCKER_BASE := ghcr.io/timescale/dev_promscale_extension
MDOX_BIN=mdox
MDOX_VALIDATE_CONFIG=
CURRENT_BRANCH?=$(shell git rev-parse --abbrev-ref HEAD)
EXTENSION_VERSION=$(shell cat EXTENSION_VERSION | tr -d '[:space:]')


.PHONY: build
build: generate
	go build -ldflags "-X 'github.com/timescale/promscale/pkg/version.Branch=${GIT_BRANCH}' -X 'github.com/timescale/promscale/pkg/version.CommitHash=${GIT_COMMIT}'" -o dist/promscale ./cmd/promscale

.PHONY: test
test: unit e2e jaeger-storage-test upgrade-test

.PHONY: unit
unit: generate prom-migrator
	go test -v -race $(shell go list ./... | grep -v tests/end_to_end_tests | grep -v tests/upgrade_tests) -timeout 40m

.PHONY: prom-migrator
prom-migrator:
	cd migration-tool && go test -v -race ./...

# traces-dataset.sz is used by ./pkg/tests/end_to_end_tests/ingest_trace_test.go
pkg/tests/testdata/traces-dataset.sz:
	wget https://github.com/timescale/promscale-test-data/raw/main/traces-dataset.sz -O ./pkg/tests/testdata/traces-dataset.sz

.PHONY: e2e
e2e: DOCKER_IMAGE?=$(shell ./scripts/fallback-docker.sh $(LOCAL_DOCKER_BASE):head-ts2-pg15 $(GHCR_DOCKER_BASE):$(CURRENT_BRANCH)-ts2-pg15 $(GHCR_DOCKER_BASE):$(EXTENSION_VERSION)-ts2-pg15)
e2e: pkg/tests/testdata/traces-dataset.sz generate
	go test -v ./pkg/tests/end_to_end_tests/ -timescale-docker-image=$(DOCKER_IMAGE)
	# TODO: Skipping multinode because tests are broken for now
	# go test -v ./pkg/tests/end_to_end_tests/ -use-multinode -timescale-docker-image=$(DOCKER_IMAGE)

.PHONY: jaeger-storage-test
jaeger-storage-test: DOCKER_IMAGE?=$(shell ./scripts/fallback-docker.sh $(LOCAL_DOCKER_BASE):head-ts2-pg15 $(GHCR_DOCKER_BASE):$(CURRENT_BRANCH)-ts2-pg15 $(GHCR_DOCKER_BASE):$(EXTENSION_VERSION)-ts2-pg15)
jaeger-storage-test:
	go test -v ./pkg/tests/end_to_end_tests/ -timescale-docker-image=$(DOCKER_IMAGE) -tags=jaeger_storage_test -run="^TestJaegerStorageIntegration/"

.PHONY: upgrade-test
upgrade-test: DOCKER_IMAGE?=$(shell ./scripts/fallback-docker.sh $(LOCAL_DOCKER_BASE):head-ts2-pg13 $(GHCR_DOCKER_BASE):$(CURRENT_BRANCH)-ts2-pg13 $(GHCR_DOCKER_BASE):$(EXTENSION_VERSION)-ts2-pg13)
upgrade-test:
	go test -v ./pkg/tests/upgrade_tests/ -timescale-docker-image=$(DOCKER_IMAGE)

.PHONY: go-fmt
go-fmt:
	go fmt ./...

.PHONY: go-lint
go-lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: shellcheck
shellcheck:
	shellcheck -S warning -f gcc $(shell find . -type f -name "*.sh")

MDOX_VALIDATE_CONFIG?=.mdox.validate.yaml
.PHONY: docs
docs:
	@echo ">> formatting and local/remote links"
	$(MDOX_BIN) fmt --soft-wraps -l --links.validate.config-file=$(MDOX_VALIDATE_CONFIG) **/*.md

.PHONY: check-docs
check-docs:
	@echo ">> checking formatting and local/remote links"
	$(MDOX_BIN) fmt --soft-wraps --check -l --links.validate.config-file=$(MDOX_VALIDATE_CONFIG) **/*.md

.PHONY: deepcopy-gen
deepcopy-gen: deepcopy-gen-install
	@echo ">> generating deepcopy code"
	deepcopy-gen -i ./pkg/dataset -h build/deepcopy-gen-header.txt -o '.'

.PHONY: deepcopy-gen-install
deepcopy-gen-install:
	@echo ">> installing deepcopy-gen"
	go install k8s.io/code-generator/cmd/deepcopy-gen@latest

.PHONY: all
all: build test go-fmt go-lint
