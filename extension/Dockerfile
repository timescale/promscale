ARG PG_VERSION_TAG=pg12
ARG TIMESCALEDB_VERSION=1.7.0
FROM timescale/timescaledb:${TIMESCALEDB_VERSION}-${PG_VERSION_TAG}

MAINTAINER Timescale https://www.timescale.com

COPY timescale_prometheus_extra.control Makefile /build/timescale-prometheus/
COPY src/*.c src/*.h /build/timescale-prometheus/src/
COPY sql/timescale-prometheus.sql /build/timescale-prometheus/sql/

RUN set -ex \
    && apk add --no-cache --virtual .build-deps \
                coreutils \
                dpkg-dev dpkg \
                gcc \
                libc-dev \
                make \
                util-linux-dev \
		clang \
		llvm \
    \
    && make -C /build/timescale-prometheus install \
    \
    && apk del .build-deps \
    && rm -rf /build
