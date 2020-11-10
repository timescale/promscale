# Docker-compose

Here you can find an initial setup for getting started

This docker-compose has 3 services:

* Prometheus
* TimescaleDB with the promscale extension
* Promscale

---

First start up the stack:
```console
$ docker-compose up -d
```

Promscale might fail at first because it needs TimescaleBD container to be up, and the database inside should be ready to receive traffic. After a few restarts(that are already configured to happen in the docker-compose), promscale should be up and running.

```console
$ docker-compose -f docker-compose.yml ps
           Name                          Command               State           Ports         
---------------------------------------------------------------------------------------------
dockercompose_prometheus_1    /bin/prometheus --config.f ...   Up      9090/tcp              
dockercompose_promscale_1     /promscale                       Up                            
dockercompose_timescaleDB_1   docker-entrypoint.sh postgres    Up      0.0.0.0:5432->5432/tcp
```

Promscale needs a database to be created previously at TimescaleDB, so we're creating a database called `timescale` in our [init.sql](./init.sql).

Prometheus is configured to scrape itself, and promscale is already configured as the remote_write and remote_read backend. After everything is up and running, you can already see Prometheus metrics, inside timescale, with your preferable Postgres Client tool (e.g. `psql`).

In this example we are searching for the metric `up`:
```console
$ PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d timescale -c "SELECT * FROM prom_metric.up;"
            time            | value | series_id |   labels    | instance_id | job_id 
----------------------------+-------+-----------+-------------+-------------+--------
 2020-11-10 18:51:33.832+00 |     1 |        85 | {114,22,43} |          22 |     43
 2020-11-10 18:51:48.832+00 |     1 |        85 | {114,22,43} |          22 |     43
 2020-11-10 18:52:03.832+00 |     1 |        85 | {114,22,43} |          22 |     43
```