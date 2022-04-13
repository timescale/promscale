# Build stage
FROM golang:1.18.1-alpine AS builder
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
    -o /go/prom-migrator ./cmd/prom-migrator

# Final image
FROM busybox
LABEL maintainer="Timescale https://www.timescale.com"
COPY --from=builder /go/prom-migrator /
ENTRYPOINT ["/prom-migrator"]
