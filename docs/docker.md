# 🐳 Docker

## 🔧 Running Docker

A docker image for the Promscale Connector is available
on Docker Hub at [timescale/promscale](https://hub.docker.com/r/timescale/promscale/).

To fetch the latest Promscale image, please refer to the [releases](https://github.com/timescale/promscale/releases)
section of Promscale github repository.

A docker image of TimescaleDB with associated extensions including the `promscale`
extension is available on Docker Hub at [`timescale/timescaledb-ha:pg14-latest`](https://hub.docker.com/r/timescale/timescaledb-ha).
You can install this via `docker pull timescale/timescaledb-ha:pg14-latest`.

The `pg14` suffix means this image is for PostgreSQL version 14. We also publish images to DockerHub for other PostgreSQL versions in case you have specific version requirements.

**Example using docker:**

First, let's create a network specific to Promscale-TimescaleDB.

```dockerfile
docker network create --driver bridge promscale-timescaledb
```

Install and run TimescaleDB with Promscale extension:

```dockerfile
docker run --name timescaledb -e POSTGRES_PASSWORD=<password> -d -p 5432:5432 --network promscale-timescaledb timescale/timescaledb-ha:pg14-latest postgres -csynchronous_commit=off
```

Finally, let's run Promscale:

```dockerfile
docker run --name promscale -d -p 9201:9201 --network promscale-timescaledb timescale/promscale:latest -db-password=<password> -db-port=5432 -db-name=postgres -db-host=timescaledb -db-ssl-mode=allow
```

If you want to run a specific version of Promscale, replace latest with the specific version you want to run. You can see all versions available in [DockerHub](https://hub.docker.com/r/timescale/promscale/tags)

**Note:** `db-ssl-mode=allow` is just for explanatory purposes. In production environments,
we advise you to use `db-ssl-mode=require` for security purposes.

**Example using docker-compose:**

You can also run Promscale with docker-compose. The `docker-compose.yaml` is available in the
[docker-compose](https://github.com/timescale/promscale/blob/master/docker-compose/docker-compose.yaml) directory of our repository.
You can start Promscale and related services simply via `docker-compose up`.

**Note:** To update Promscale with docker-compose, you need to stop the Promscale that is currently running using
`docker-compose stop` and then pull the image with the tag you want to upgrade to with `docker pull timescale/promscale:<version-tag>`.
This will pull the respective image to your local docker registry. You can then run the updated image with `docker-compose up`.

## 🕞 Setting up cron jobs for Timescale DB 1.X

If you are using TimescaleDB 1.X you need to make sure the `execute_maintenance()`
procedure is run on a regular basis (e.g. via cron). This is not needed if you are
using TimescaleDB 2.X because it has native support for task scheduling which
is automatically configured by Promscale to run the `execute_maintenance()`
procedure.

We recommend executing it every
30 minutes. This is necessary to execute maintenance tasks such as enforcing
data retention policies according to the configured policy.

**Example:**

```bash
docker exec \
   --user postgres \
   timescaledb \
      psql \
        -c "CALL execute_maintenance();"
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

For more information about configuration, consult our documentation [here](/docs/configuration.md).

## 🛠 Building from source

You can build the Docker container using the [Dockerfile](../build/Dockerfile).

## Upgrading from the previous alpine image

Previously, our recommended image was located at [`timescaledev/promscale-extension`](https://hub.docker.com/r/timescaledev/promscale-extension).
It was based on the [Alpine docker image for PostgreSQL](https://github.com/docker-library/postgres/blob/e8ebf74e50128123a8d0220b85e357ef2d73a7ec/12/alpine/Dockerfile).
Because of [collation bugs](https://github.com/docker-library/postgres/issues/327) and other issues we have now switched our recommendation to the Debian-based image above.

Our previous Alpine-based image will continue to be supported but all new installations should switch to the `timescaledb-ha` image specified above.

You can migrate to the Debian version by doing the following (please note: this can be a lengthy process and involves downtime):

1. Use `docker inspect` to determine the data volumes used by your database for the data directory.
2. Shutdown all Promscale Connectors.
3. Shutdown the original database docker image while preserving the volume mount for the data directory.
   You will need to mount this same directory in the new image.
4. Change the ownership of the data-directory to the postgres user and group in the new image. For example:

   ```
   docker run -v <data_dir_volume_mount>:/var/lib/postgresql/data timescale/timescaledb-ha:pg14-latest chown -R postgres:postgres /var/lib/postgresql/data
   ```
5. Start the new docker container with the same volume mounts as what the original container used.
6. Connect to the new database using psql and reindex all the data that has data
   that is collatable. This is necessary because the collation in the Alpine image
   is broken and so BTREE-based indexes will be incorrect until they are reindexed.
   It is extremely important to execute this step before ingesting new data to
   avoid data corruption. Note: This process can take a long time depending on how
   much indexed textual data the database has. You should use the following query to
   reindex all the necessary indexes:

   ```
     DO $$DECLARE r record;
     BEGIN
       FOR r IN
         SELECT DISTINCT indclass
             FROM (SELECT indexrelid::regclass indclass, unnest(string_to_array(indcollation::text, ' ')) coll FROM pg_catalog.pg_index) sub
             INNER JOIN pg_catalog.pg_class c ON (c.oid = sub.indclass)
             WHERE coll !='0' AND c.relkind != 'I'
       LOOP
        EXECUTE 'REINDEX INDEX ' || r.indclass;
     END LOOP;
   END$$;
   ```

7. Restart the Promscale Connector

If you are using Kubernetes instead of plain docker you should:
1. Shutdown the Promscale Connector pods
2. Change the database pod to use the debian docker image and restart it.
3. Execute jobs for the script in steps 4 and 6 above.
4. Restart the Promscale Connector pods.
