# Build stage
FROM golang:1.14.1-alpine AS builder
COPY ./pkg timescale-prometheus-connector/pkg
COPY ./cmd timescale-prometheus-connector/cmd
COPY ./go.mod timescale-prometheus-connector/go.mod
COPY ./go.sum timescale-prometheus-connector/go.sum
RUN apk update && apk add --no-cache git \
    && cd timescale-prometheus-connector \
    && go mod download \
    && CGO_ENABLED=0 go build -a --ldflags '-w' -o /go/timescale-prometheus ./cmd/timescale-prometheus

# Final image
FROM busybox
LABEL maintainer="Timescale https://www.timescale.com"
COPY --from=builder /go/timescale-prometheus /
ENTRYPOINT ["/timescale-prometheusß"]
