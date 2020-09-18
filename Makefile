SHELL := /bin/bash

VERSION ?= $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || \
            echo v0)

vendor:
	go mod tidy
	go mod vendor
	go mod verify