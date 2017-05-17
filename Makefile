VERSION=$(shell git describe --always | sed 's|v\(.*\)|\1|')
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)
OS:=$(shell uname -s | awk '{ print tolower($$1) }')
ARCH=amd64
REGISTRY=registry.iobeam.com
ORGANIZATION=timescale

ifeq ($(shell uname -m), i386)
	ARCH=386
endif

SOURCES:= \
	postgresql/client.go \
	main.go

TEST_SOURCES:=

TARGET:=prometheus-postgresql-adapter

.PHONY: all clean docker test prepare-for-docker-build

all: $(TARGET) version.properties

version.properties:
	@echo "version=${VERSION}" > version.properties

.target_os:
	@echo $(OS) > .target_os

$(TARGET): .target_os $(SOURCES)
	GOOS=$(OS) GOARCH=${ARCH} CGO_ENABLED=0 go build -a -installsuffix cgo --ldflags '-w' -o $@ main.go

prepare-for-docker-build:
	$(eval OS=linux)
ifneq ($(shell cat .target_os 2>/dev/null),linux)
	rm -f $(TARGET) .target_os
endif

docker: prepare-for-docker-build prometheus-postgresql-adapter version.properties
	docker build -t $(ORGANIZATION)/$(TARGET):latest .
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${VERSION}
	docker tag $(ORGANIZATION)/$(TARGET):latest $(ORGANIZATION)/$(TARGET):${BRANCH}
	docker tag $(ORGANIZATION)/$(TARGET):latest ${REGISTRY}/$(ORGANIZATION)/$(TARGET):latest
	docker tag $(ORGANIZATION)/$(TARGET):latest ${REGISTRY}/$(ORGANIZATION)/$(TARGET):${VERSION}
	docker tag $(ORGANIZATION)/$(TARGET):latest ${REGISTRY}/$(ORGANIZATION)/$(TARGET):${BRANCH}

push: docker
	docker push $(ORGANIZATION)/$(TARGET):latest
	docker push $(ORGANIZATION)/$(TARGET):${VERSION}
	docker push $(ORGANIZATION)/$(TARGET):${BRANCH}

test: ${TEST_SOURCE}
	go test -race ./postgresql

clean:
	go clean
	rm -f *~ $(TARGET) version.properties .target_os
