FROM openshift/origin-release:golang-1.15 AS builder

WORKDIR /timescale-prometheus
COPY ./pkg ./pkg
COPY ./cmd ./cmd
COPY ./go.mod ./go.mod
COPY ./go.sum ./go.sum
COPY ./vendor ./vendor

ENV GO111MODULE="on"

# TODO: do the actual building of the binary in Makefile target
RUN CGO_ENABLED=0 \
    go build -a --mod=vendor --ldflags '-w' \
    -o /go/timescale-prometheus ./cmd/timescale-prometheus

FROM centos:8
USER 3001
COPY --from=builder /go/timescale-prometheus /
ENTRYPOINT ["/timescale-prometheus"]
