# Build stage
FROM golang:1.14.1-alpine AS builder
COPY ./.git timescale-prometheus/.git
COPY ./pkg timescale-prometheus/pkg
COPY ./cmd timescale-prometheus/cmd
COPY ./go.mod timescale-prometheus/go.mod
COPY ./go.sum timescale-prometheus/go.sum
RUN apk update && apk add --no-cache git \
    && cd timescale-prometheus \
    && go mod download \
    && GIT_COMMIT=$(git rev-list -1 HEAD) \
    && VERSION=$(git describe $GIT_COMMIT) \
    && CGO_ENABLED=0 go build -a \
    --ldflags '-w' --ldflags "-X main.CommitHash=$GIT_COMMIT" --ldflags "-X main.Version=$VERSION" \
    -o /go/timescale-prometheus ./cmd/timescale-prometheus

# Final image
FROM busybox
LABEL maintainer="Timescale https://www.timescale.com"
COPY --from=builder /go/timescale-prometheus /
ENTRYPOINT ["/timescale-prometheus"]
