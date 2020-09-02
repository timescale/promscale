ARG PG_VERSION_TAG=pg12
ARG TIMESCALEDB_VERSION=1.7.1
FROM timescale/timescaledb:${TIMESCALEDB_VERSION}-${PG_VERSION_TAG}

MAINTAINER Timescale https://www.timescale.com

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
                git \
                llvm-dev clang-libs \
                rust cargo

RUN set -ex \
    && git clone  --branch v1.9 --depth 1 \
         https://github.com/dimitri/pgextwlist.git /pgextwlist \
    && cd /pgextwlist \
    && make \
    && make install \
    && cp /pgextwlist/pgextwlist.so `pg_config --pkglibdir`/plugins \
    && rm -rf /pgextwlist

COPY timescale_prometheus_extra.control Makefile dependencies.makefile /build/timescale-prometheus/
COPY src/*.c src/*.h /build/timescale-prometheus/src/
COPY Cargo.* /build/timescale-prometheus/
COPY src/*.rs /build/timescale-prometheus/src/
COPY sql/*.sql /build/timescale-prometheus/sql/

RUN set -ex \
    && make -C /build/timescale-prometheus rust EXTRA_RUST_ARGS=--features=parse_headers \
    && make -C /build/timescale-prometheus install \
    \
    && apk del .build-deps \
    && rm -rf /build

COPY 003-enable-pgextwlist.sh /docker-entrypoint-initdb.d/
