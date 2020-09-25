ARG PG_VERSION_TAG=pg12
ARG TIMESCALEDB_VERSION=1.7.3
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
    && mkdir `pg_config --pkglibdir`/plugins \
    && cp /pgextwlist/pgextwlist.so `pg_config --pkglibdir`/plugins \
    && rm -rf /pgextwlist

COPY promscale.control Makefile dependencies.makefile /build/promscale/
COPY src/*.c src/*.h /build/promscale/src/
COPY Cargo.* /build/promscale/
COPY src/*.rs /build/promscale/src/
COPY sql/*.sql /build/promscale/sql/

RUN set -ex \
    && make -C /build/promscale rust EXTRA_RUST_ARGS=--features=parse_headers \
    && make -C /build/promscale install \
    \
    && apk del .build-deps \
    && rm -rf /build
