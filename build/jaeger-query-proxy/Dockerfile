# Build stage
FROM golang:1.15.4-alpine AS builder
COPY ./.git build/.git
COPY ./pkg  build/pkg
COPY ./cmd  build/cmd
COPY ./go.mod build/go.mod
COPY ./go.sum build/go.sum
RUN apk update && apk add --no-cache git \
    && cd build \
    && go mod download \
    && GIT_COMMIT=$(git rev-list -1 HEAD) \
    && CGO_ENABLED=0 go build -a \
    --ldflags '-w' --ldflags "-X version.CommitHash=$GIT_COMMIT" \
    -o /go/jaeger-query-proxy ./cmd/jaeger-query-proxy

# Final image
FROM jaegertracing/jaeger-query:1.27

ENV SPAN_STORAGE_TYPE=grpc-plugin \
    GRPC_STORAGE_PLUGIN_BINARY=/tmp/promscale-jaeger-plugin \
    GRPC_STORAGE_PLUGIN_CONFIGURATION_FILE=/configs/jaeger-promscale-query.yaml

# This is silly, but it's important that tempo-query gets copied into /tmp
#  b/c it forces a /tmp dir to exist which hashicorp plugins depend on

RUN cd / && mkdir configs
COPY --from=builder /go/jaeger-query-proxy /tmp/promscale-jaeger-plugin