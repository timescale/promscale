GIT_COMMIT ?= $(shell git rev-list -1 HEAD)
GIT_BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
LOCAL_DOCKER_BASE := local/dev_promscale_extension
GHCR_DOCKER_BASE := ghcr.io/timescale/dev_promscale_extension
MDOX_BIN=mdox
MDOX_VALIDATE_CONFIG=

.PHONY: build
build: generate
	go build -ldflags "-X 'github.com/timescale/promscale/pkg/version.Branch=${GIT_BRANCH}' -X 'github.com/timescale/promscale/pkg/version.CommitHash=${GIT_COMMIT}'" -o dist/promscale ./cmd/promscale

.PHONY: test
test: unit e2e upgrade-test

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
e2e: CURRENT_BRANCH?=$(shell git branch --show-current | sed 's#/#-#')
e2e: EXTENSION_VERSION=$(shell cat EXTENSION_VERSION | tr -d '[:space:]')
e2e: DOCKER_IMAGE?=$(shell ./scripts/fallback-docker.sh $(LOCAL_DOCKER_BASE):head-ts2-pg14 $(GHCR_DOCKER_BASE):$(CURRENT_BRANCH)-ts2-pg14 $(GHCR_DOCKER_BASE):$(EXTENSION_VERSION)-ts2-pg14)
e2e: pkg/tests/testdata/traces-dataset.sz generate
	go test -timeout=20m -v ./pkg/tests/end_to_end_tests/ -timescale-docker-image=$(DOCKER_IMAGE)
	go test -timeout=20m -v ./pkg/tests/end_to_end_tests/ -use-timescaledb=false -timescale-docker-image=$(DOCKER_IMAGE)
	# TODO: Skipping multinode because tests are broken for now
	# go test -v ./pkg/tests/end_to_end_tests/ -use-multinode -timescale-docker-image=$(DOCKER_IMAGE)

.PHONY: upgrade-test
upgrade-test: CURRENT_BRANCH?=$(shell git branch --show-current | sed 's#/#-#')
upgrade-test: EXTENSION_VERSION=$(shell cat EXTENSION_VERSION | tr -d '[:space:]')
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

.PHONY: all
all: build test go-fmt go-lint
