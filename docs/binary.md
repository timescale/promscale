# 🔟 Binaries

## 🔧 Installing pre-built binaries

A full Promscale installation consists of both the promscale connector, and a TimescaleDB instance with the Promscale
Extension installed. We provide pre-built binaries (and packages) for the both.

### Setup TimescaleDB with Promscale Extension

The simplest way to get started with TimescaleDB and the Promscale Extension is to install both from the TimescaleDB
package repository. You can find instructions on how to install TimescaleDB for your favourite distribution
[here](https://docs.timescale.com/install/latest/self-hosted/). Once you have installed TimescaleDB for a specific
Postgres version from our package repository, you can simply install the Promscale Extension for the same Postgres
version, e.g.:

```bash
apt-get install -y promscale-extension-postgresql-14
```

### Setup the Promscale Connector

Similarly, you can get the latest version of the Promscale Connector from the official TimescaleDB package repository.

```bash
apt-get install -y promscale
```

If you're using systemd, you can use our unit and an associated configuration file to configure the Promscale connector.

Otherwise, you can start the Promscale connector directly:

```bash
promscale --db-host <DB_HOSTNAME> --db-port <DB_PORT> --db-name <DBNAME> --db-password <DB-Password> --db-ssl-mode allow
```

Note that the flags `db-name` and `db-password` refer to the name and password of your TimescaleDB database.

Further note that the command above starts the Promscale connector in "SSL allowed" mode. For production setups it's recommended to start Promscale in "SSL required" mode enabled. To do so, configure your TimescaleDB instance with ssl certificates and drop the `--db-ssl-mode` flag. Promscale will then authenticate via SSL by default.

## 🔥 Configuring Prometheus to use this remote storage connector

You must tell prometheus to use this remote storage connector by adding
the following lines to `prometheus.yml`:

```yaml
remote_write:
  - url: "http://<connector-address>:9201/write"
remote_read:
  - url: "http://<connector-address>:9201/read"
    read_recent: true
```

**Note:** Setting `read_recent` to `true` will make Prometheus query data from Promscale for all PromQL queries. This is highly recommended.

You can configure Prometheus remote-write with our recommended configurations from [here](/docs/configuring_prometheus.md).

## ⚙️ Configuration

The Promscale Connector binary is configured through either CLI flags, environment variables, or a YAML configuration file.
Precedence goes like this: CLI flag value, if not set, environment variable value, if not set, configuration file value, if not set, default value.

All environment variables are prefixed with `PROMSCALE`.

Configuration file is a YAML file where the keys are CLI flag names and values are their respective flag values.

The list of available cli flags is available in [here](/docs/configuration.md) in
our docs or by running with the `-h` flag (e.g. `promscale -h`)

## 🛠 Building from source

Before building, make sure the following prerequisites are installed:

* [Go](https://golang.org/dl/)

The Promscale Connector is a Go project managed by go
modules. You can download it in
any directory and on the first build it will download it's required
dependencies.

```bash
# Fetch the source code of Promscale in any directory
$ git clone git@github.com:timescale/promscale.git
$ cd ./promscale

# Install the Promscale Connector binary (will automatically detect and download)
# dependencies.
$ cd cmd/promscale
$ go install

# Building without installing will also fetch the required dependencies
$ go build ./...
```
