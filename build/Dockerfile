# Build stage
FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.15.4-alpine AS builder
RUN apk update && apk add --no-cache git
WORKDIR /promscale
COPY ./go.mod ./go.sum ./
RUN go mod download
COPY ./.git .git/
COPY ./pkg pkg/
COPY ./cmd cmd/
ARG TARGETOS
ARG TARGETARCH
RUN go generate ./...
RUN GIT_COMMIT=$(git rev-list -1 HEAD) \
    && GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} CGO_ENABLED=0 go build -a \
    --ldflags '-w' --ldflags "-X version.CommitHash=$GIT_COMMIT" \
    -o /bin/promscale ./cmd/promscale

# Final image
FROM busybox
ENV PROMSCALE_PKG=docker
LABEL maintainer="Timescale https://www.timescale.com"
COPY --from=builder /bin/promscale /
ENTRYPOINT ["/promscale"]
