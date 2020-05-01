#!/usr/bin/env bash
set -ex

sed -i -e "s/#shared_preload_libraries/shared_preload_libraries/" \
    /var/lib/postgresql/data/postgresql.conf \

sed -i \
    -e "s/shared_preload_libraries = '/shared_preload_libraries = 'pgextwlist,/" \
    /var/lib/postgresql/data/postgresql.conf

echo "extwlist.extensions = 'timescale_prometheus_extra,timescaledb'" >> \
    /var/lib/postgresql/data/postgresql.conf
