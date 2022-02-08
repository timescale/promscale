# 🔟 Binaries

## 🔧 Installing pre-built binaries

You can download pre-built binaries for the Promscale
Connector [from our release page](https://github.com/timescale/promscale/releases).

Download Promscale

```
curl -L -o promscale https://github.com/timescale/promscale/releases/download/<VERSION>/<PROMSCALE_DISTRIBUTION>
```

Grant executable permissions to Promscale:

```
chmod +x promscale
```

To deploy Promscale, run the following command:
```
./promscale --db-host <DB_HOSTNAME> --db-port <DB_PORT> --db-name <DBNAME> --db-password <DB-Password> --db-ssl-mode allow
```
Note that the flags `db-name` and `db-password` refer to the name and password of your TimescaleDB database.

Further note that the command above is to deploy Promscale with SSL mode being allowed but not required. To deploy Promscale with SSL mode enabled, configure your TimescaleDB instance with ssl certificates and drop the `--db-ssl-mode` flag.  Promscale will then authenticate via SSL by default.

We recommend installing the [promscale](https://github.com/timescale/promscale_extension/releases)
PostgreSQL extension into the TimescaleDB database you are connecting to.
Instructions on how to compile and install the extension are in the
extensions [README](https://github.com/timescale/promscale_extension/blob/master/Readme.md). While this isn't a requirement, it
does optimize certain queries.
Please note that the extension requires Postgres version 12 of newer.

## 🕞 Setting up cron jobs for Timescale DB 1.X

If you are using TimescaleDB 1.X you need to make sure the `execute_maintenance()`
procedure on a regular basis (e.g. via cron). We recommend executing it every
30 minutes. This is necessary to execute maintenance tasks such as enforcing
data retention policies according to the configured policy.

**Note:** This is not needed if you are using TimescaleDB 2.X because it has native support for task scheduling which
is automatically configured by Promscale to run the `execute_maintenance()` procedure.

Copy the code snippet from the file [docs/scripts/prom-execute-maintenance.sh](/docs/scripts/prom-execute-maintenance.sh) and add the database password in place of `<PASSWORD>`.

Create an other script with the code snippet from the file [docs/scripts/install-crontab.sh](/docs/scripts/install-crontab.sh) and make sure to configure the absolute path to `prom-execute-maintenance.sh` in the script. This script will create a crontab for the above defined task.

Then, grant executable privileges to both files:

```
chmod +x prom-execute-maintenance.sh
chmod +x install-crontab.sh
```

Install the cron job:

```
./install-crontab.sh
```

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
