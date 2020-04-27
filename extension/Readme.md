# Timescale-Prometheus Extra Extension #

This [Postgres extension](https://www.postgresql.org/docs/12/extend-extensions.html)
contains support functions to improve the performance of Timescale-Prometheus.
While Timescale-Prometheus will run without it, adding this extension will
cause it perform better.

## Installation ##

The extension is installed by default on the timescale-prometheus docker image,
and will be installed automatically by timescale-prometheus, if it has
permission.
