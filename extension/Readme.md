# Timescale-Prometheus Extra Extension #

This [Postgres extension](https://www.postgresql.org/docs/12/extend-extensions.html)
contains support functions to improve the performance of Timescale-Prometheus.
While Timescale-Prometheus will run without it, adding this extension will
cause it to perform better.

## Installation ##

The extension is installed by default on the
[`timescaledev/timescale_prometheus_extra:latest-pg12`](https://hub.docker.com/r/timescaledev/timescale_prometheus_extra) docker image.

To compile and install from source run: `make && make install`.

This extension will be installed automatically by the timescale-prometheus connector.
