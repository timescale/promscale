VERSION=$(shell git describe --always | sed 's|v\(.*\)|\1|')
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
OS:=$(shell uname -s | awk '{ print tolower($$1) }')
ARCH=amd64
ORGANIZATION=timescale

ifeq ($(shell uname -m), i386)
	ARCH=386
endif

# Packages for running tests
PKGS:= $(shell go list ./... | grep -v /vendor)

SOURCES:=$(shell find . -name '*.go'  | grep -v './vendor')

TARGET:=prometheus-postgresql-adapter

.PHONY: all clean build docker-image docker-push test prepare-for-docker-build

all: $(TARGET) version.properties

version.properties:
	@echo "version=${VERSION}" > version.properties

.target_os:
	@echo $(OS) > .target_os

build: $(TARGET)

$(TARGET): .target_os $(SOURCES)
	$(if $(shell command -v dep 2> /dev/null),$(info Found golang/dep),$(error Please install golang/dep))
	dep ensure
	GOOS=$(OS) GOARCH=${ARCH} CGO_ENABLED=0 go build -a -installsuffix cgo --ldflags '-w' -o $@ 

prepare-for-docker-build:
	$(eval OS=linux)
ifneq ($(shell cat .target_os 2>/dev/null),linux)
	rm -f $(TARGET) .target_os
endif

docker-image: prepare-for-docker-build prometheus-postgresql-adapter version.properties
	docker build -t $(ORGANIZATION)/$(TARGET):latest .
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${VERSION}
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${BRANCH}

docker-push: docker-image
	docker push $(ORGANIZATION)/$(TARGET):latest
	docker push $(ORGANIZATION)/$(TARGET):${VERSION}
	docker push $(ORGANIZATION)/$(TARGET):${BRANCH}

test:
	GOCACHE=off go test -v -race $(PKGS) -args -database=false

clean:
	go clean
	rm -f *~ $(TARGET) version.properties .target_os
