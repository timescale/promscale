# Build stage
FROM golang:1.12.9-alpine AS builder
COPY ./pkg prometheus-adapter/pkg
COPY ./cmd prometheus-adapter/cmd
COPY ./go.mod prometheus-adapter/go.mod
COPY ./go.sum prometheus-adapter/go.sum
RUN apk update && apk add --no-cache git \
    && cd prometheus-adapter \
    && go mod download \
    && CGO_ENABLED=0 go build -a --ldflags '-w' -o /go/prometheus-postgresql-adapter ./cmd/prometheus-postgresql-adapter

# Final image
FROM busybox
MAINTAINER Timescale https://www.timescale.com
COPY --from=builder /go/prometheus-postgresql-adapter /
ENTRYPOINT ["/prometheus-postgresql-adapter"]
