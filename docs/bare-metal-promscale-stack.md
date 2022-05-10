# Deploying Promscale on bare metal

This document is a guide to setup and run Promscale on bare metal (i.e a non-containerized environment) with **TimescaleDB > 2.0**.

**Note**: If you prefer running your applications in containers, we recommend installing Promscale via [TOBS: The Observability Stack for Kubernetes](https://github.com/timescale/tobs) or [Docker](https://github.com/timescale/promscale/blob/master/docs/docker.md).

### Overview

**Target Operating System**
The instructions below are for Ubuntu 18.04 or later (excluding obsoleted versions), but can easily be modified to deploy Promscale on the operating system of your choice. You can modify the instructions by choosing binaries of TimescaleDB, Promscale, Prometheus & Grafana which are specific to your target operating system.

There are 3 steps to deploy Promscale on bare metal:

<ol>
<li> Deploy TimescaleDB </li>
<li> Deploy Promscale </li>
<li> Deploy Prometheus </li>
</ol>

Instructions to deploy and run Grafana are included as an optional, but commonly used fifth step.

### 1. Deploying TimescaleDB

#### Build and install TimescaleDB

Follow the steps on timescaledb installation (instructions for other platforms available [on the TimescaleDB install page](https://docs.timescale.com/latest/getting-started/installation))

**If you don't already have PostgreSQL installed**, add PostgreSQL's third party repository to get the latest PostgreSQL packages (if you are using Ubuntu older than 19.04):

```
# `lsb_release -c -s` should return the correct codename of your OS
echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -c -s)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list

wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

sudo apt-get update
```

Next, add TimescaleDB's third party repository and install TimescaleDB, which will download any dependencies it needs from the PostgreSQL repo:

```
# Add our PPA
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update

# Now install appropriate package for PG version
sudo apt install timescaledb-postgresql-12
```

#### Configure TimescaleDB

Configure your database using `timescaledb-tune`. This will ensure that TimescaelDB is properly added to the parameter `shared_preload_libraries` as well as offer suggestions for tuning memory, parallelism, and other settings:

```
sudo timescaledb-tune

# Restart PostgreSQL instance
sudo service postgresql restart
```

#### Making Promscale Extension Available (optional)

The Promscale extension contains support functions to improve performance of Promscale. While Promscale will run without it, adding this extension will support it to perfrom better.

Follow the steps to compile Promscale extension from source available on [Promscale extension page](https://github.com/timescale/promscale_extension).

### 2. Deploying Promscale

Download Promscale

```
LATEST_VERSION=$(curl -s https://api.github.com/repos/timescale/promscale/releases/latest | grep "tag_name" | cut -d'"' -f4)
curl -L -o promscale "https://github.com/timescale/promscale/releases/download/${LATEST_VERSION}/promscale_${LATEST_VERSION}_Linux_x86_64"
```

Grant executable permissions to Promscale:

```
chmod +x promscale
```

To deploy Promscale, run the following command:

```
./promscale --db-name <DBNAME> --db-password <DB-Password> --db-ssl-mode allow
```

Note that the flags `db-name` and `db-password` refer to the name and password of your TimescaleDB database from Step 1.

Furthermore, note that the command above is to deploy Promscale with SSL mode as optional. To deploy Promscale with SSL mode enabled, configure your TimescaleDB instance with ssl certificates and set the `--db-ssl-mode` flag to `require`. Promscale will then force authentication via SSL.

### 3. Deploying Prometheus

**Note: Replace the versions of Prometheus listed here with the latest available version numbers**

Download Prometheus:

```
LATEST_VERSION=$(curl -s https://api.github.com/repos/prometheus/prometheus/releases/latest | grep "tag_name" | cut -d'"' -f4 | cut -c2- )
curl -O -L "https://github.com/prometheus/prometheus/releases/download/v${LATEST_VERSION}/prometheus-${LATEST_VERSION}.linux-amd64.tar.gz"
```

Uncompress the tarball and change directory into the prometheus directory:

```
tar -xzvf prometheus-${LATEST_VERSION}.linux-amd64.tar.gz
```

```
cd prometheus-${LATEST_VERSION}.linux-amd64
```

Edit the `prometheus.yml` file in order to add **Promscale** as a `remote_write` and `remote_read` endpoint for Prometheus:

```
remote_write:
 - url: "http://<promscale-host>:9201/write"
 
remote_read:
 - url: "http://<promscale-host>:9201/read"
   read_recent: true
```

Run Prometheus using the following command:

```
./prometheus
```

**Note:** Prometheus will load with the configuration defined by the `prometheus.yml` residing in the same directory as the `prometheus` executable.

### Access TimescaleDB

To access TimescaleDB, login to your PostgresSQL database using `psql` as user `postgres`:

```
su - postgres
```

To open a connection to your TimescaleDB instance via psql, run:

```
psql
```

### Deploying Grafana (optional)

Instructions to deploy and run Grafana are included as an optional, but commonly used fifth step. Grafana can be used to visualize data using Promscale as a datasource.

**Installing Grafana**
First, download Grafana:

```
LATEST_VERSION=$(curl -s https://api.github.com/repos/grafana/grafana/releases/latest | grep "tag_name" | cut -d'"' -f4 | cut -c2- )
wget https://dl.grafana.com/oss/release/grafana-${LATEST_VERSION}.linux-amd64.tar.gz
```

Next, uncompress the tarball and change directory into the grafana `bin` directory:

```
tar -zxvf grafana-${LATEST_VERSION}.linux-amd64.tar.gz
```

```
cd grafana-${LATEST_VERSION}/bin
```

To run Grafana, run the following command:

```
./grafana-server
```

**Configuring Promscale as a Grafana datasource**

To configure Promscale as a Prometheus datasource in Grafana, use the following host URL:

```
http://<promscale-host>:9201
```

All other parameters are left as default.

To configure TimescaleDB as a PostgreSQL / TimescaleDB datasource in Grafana, use the host URL and database credentials which were used to setup TimescaleDB in Step 1.

### Next Steps

Congratulations, you've successfully deployed Promscale, Prometheus and Grafana on bare metal. You can explore your metrics data by connecting to your TimescaleDB instance (as shown in Step 1 using `psql`) or through visuals in Grafana.

To continue your Promscale learning journey, check out:

* [Promscale overview video](https://youtu.be/FWZju1De5lc)
* [Analyzing metrics data using SQL](https://github.com/timescale/promscale/blob/master/docs/sql_schema.md)
* [Promscale Features](https://github.com/timescale/promscale#-features)

Should you have any trouble following the Promscale documentation, please submit an issue describing your problem and how to reproduce it. The Promscale team are active and happy to help!
