# Promscale Extension #

This [Postgres extension](https://www.postgresql.org/docs/12/extend-extensions.html)
contains support functions to improve the performance of Promscale.
While Promscale will run without it, adding this extension will
cause it to perform better.

## Requirements ##

To run the extension:
- PostgreSQL version 12 or newer.

To compile the extension:
- Header files for PostgreSQL version 12 or newer. Headers are included in the `postgresql-server-dev-12` package in Debian or Ubuntu. For other platforms see the PostgreSQL [download page](https://www.postgresql.org/download/).
- [Rust compiler](https://www.rust-lang.org/tools/install)

## Installation ##

The extension is installed by default on the
[`timescaledev/promscale-extension:latest-pg12`](https://hub.docker.com/r/timescaledev/promscale-extension) docker image.

To compile and install from source run: `make && make install`.

This extension will be created via `CREATE EXTENSION` automatically by the Promscale connector and should not be created manually.

## Common Compilation Issues ##

- `cargo: No such file or directory` means the [Rust compiler](https://www.rust-lang.org/tools/install) is not installed
- `postgres.h` not found means that you are missing the PostgreSQL header files. If using Debian or Ubuntu simply install the `postgresql-server-dev-12` package. For other platforms see the PostgreSQL [download page](https://www.postgresql.org/download/).
