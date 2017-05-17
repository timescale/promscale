FROM busybox

MAINTAINER Erik Nordström <erik@timescale.com>

COPY prometheus-postgresql-adapter /

ENTRYPOINT ["/prometheus-postgresql-adapter"]