# 🐳 Docker

## 🔧 Running Docker

A docker image for the Promscale Connector is available
on Docker Hub at [timescale/promscale](https://hub.docker.com/r/timescale/promscale/).

To fetch the latest Promscale image, please refer to the [releases](https://github.com/timescale/promscale/releases)
section of Promscale github repository.

A docker image of TimescaleDB with the `promscale`
extension is available at on Docker Hub at
[`timescaledev/promscale-extension:latest-ts2-pg13`](https://hub.docker.com/r/timescaledev/promscale-extension). You can
install this via `docker pull timescaledev/promscale-extension:latest-ts2-pg13`.

The `ts2-pg13` suffix means TimescaleDB version 2 on PostgreSQL version 13. We also publish images to DockerHub for other combinations of TimescaleDB and PostgreSQL versions in case you have specific version requirements.

**Example using docker:**

First, let's create a network specific to Promscale-TimescaleDB.

```dockerfile
docker network create --driver bridge promscale-timescaledb
```

Install and run TimescaleDB with Promscale extension:
```dockerfile
docker run --name timescaledb -e POSTGRES_PASSWORD=<password> -d -p 5432:5432 --network promscale-timescaledb timescaledev/promscale-extension:latest-ts2-pg13 postgres -csynchronous_commit=off
```

Finally, let's run Promscale:
```dockerfile
docker run --name promscale -d -p 9201:9201 --network promscale-timescaledb timescale/promscale:latest -db-password=<password> -db-port=5432 -db-name=postgres -db-host=timescaledb -db-ssl-mode=allow
```
If you want to run a specific version of Promscale, replace latest with the specific version you want to run. You can see all versions available in [DockerHub](https://hub.docker.com/r/timescale/promscale/tags)


**Note:** `db-ssl-mode=allow` is just for explanatory purposes. In production environments,
we advise you to use `db-ssl-mode=require` for security purposes.

This should make Promscale running along with TimescaleDB.

**Example using docker-compose:**

You can also run Promscale also by using docker-compose. The `docker-compose.yaml` is available in the
[docker-compose](https://github.com/timescale/promscale/blob/master/docker-compose/docker-compose.yaml) directory of our repository.
You can start Promscale and related services simply via `docker-compose up`.

For updating Promscale in this method, you need to stop the Promscale that is currently running using
`docker-compose stop` and then pull the image with the tag you want to upgrade to with `docker pull timescale/promscale:<version-tag>`.
This will pull the respective image to your local docker registry. You can then run the updated image with `docker-compose up`.

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

The list of available cli flags is available in [here](/docs/cli.md) in
our docs or by running with the `-h` flag (e.g. `promscale -h`).

## 🛠 Building from source

You can build the Docker container using the [Dockerfile](build/Dockerfile).
