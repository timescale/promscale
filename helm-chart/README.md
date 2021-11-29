# Promscale Helm chart

This directory contains a Helm chart to deploy the Promscale Connector on Kubernetes.
If you are looking to deploy an entire observability suite including Prometheus,
Promscale, Grafana and other tools, we suggest using
[The Observability Suite for Kubernetes (tobs)](https://github.com/timescale/tobs).

This chart will do the following:

* Create a Kubernetes Deployment (by default) with one pod
  * The pod has a container created using the [Promscale Connector Docker Image][docker-image]
* Create a Kubernetes Service exposing access to the Connector pods
  * By default a LoadBalancer, but can be disabled to only a ClusterIP with a configurable port
* Create a Kubernetes CronJob that deletes the data chunks that fall out of the retention period

## Prerequisites

For promscale to work correctly it needs a set of data to connect to timescale database. This 
configuration can be supplied in two ways either by using DB URI or by specifying connection
parameters. Options are mutually exclusive and specifying URI takes priority.

### Using DB URI

You can use db uri to connect to TimescaleDB. To do so, specify the URI in values.yaml as follows:
```yaml
connection:
  uri: <TIMESCALE_DB_URI>
```

### Using Connection Parameters

Instead of using db uri, you can specify all parameters necessary for connecting promscale to timescaledb using `connection` map.
Bear in mind that timescale database should exist before starting promscale or at least credentials should be available.

Following are the default configuration values:

```yaml
connection:
  user: postgres
  password: ""
  host: db.timescale.svc.cluster.local
  port: 5432
  sslMode: require
  dbName: timescale
```

## Installing

To install the chart with the release name `my-release`:
```shell script
helm install --name my-release .
```

You can override parameters using the `--set key=value[,key=value]` argument
to `helm install`, e.g. to install the chart with specifying a previously created
secret `timescale-secret` and an existing TimescaleDB instance:
```shell script
helm install --name my-release . \
      --set connection.password.secretTemplate="timescale-secret"
      --set connection.host.nameTemplate="timescaledb.default.svc.cluster.local"
```

You can also install by referencing the db uri secret created previously:

```shell script
helm install --name my-release . \
      --set connection.dbURI.secretTemplate="timescale-secret"
```
 
Alternatively, a YAML file the specifies the values for the parameters can be provided
while installing the chart. For example:
```shell script
helm install --name my-release -f myvalues.yaml .
```

## Configuration

|       Parameter                   |           Description                       |               Default              |
|-----------------------------------|---------------------------------------------|------------------------------------|
| `image`                           | The image (with tag) to pull                | `timescale/promscale`   |
| `imagePullPolicy`                 | The image pull policy                       | `IfNotPresent`   |
| `replicaCount`                    | Number of pods for the connector            | `1`                                |
| `upgradeStrategy`                 | Promscale deployment upgrade strategy, By default set to `Recreate` as during Promscale upgrade we expect no Promscale to be connected to TimescaleDB       | `Recreate`                                |
| `connection.user`                 | Username to connect to TimescaleDB with     | `postgres`                         |
| `connection.password`             | The DB password for user specified in `connection.user` | "" |
| `connection.uri`                  | DB uri string used for database connection. When not empty it takes priority over other settings in `connection` map. | "" |
| `connection.host`                 | Hostname of timescaledb instance            | `db.timescaledb.svc.cluster.local` |
| `connection.port`                 | Port the db listens to                      | `5432`                             |
| `connection.dbName`               | Database name in TimescaleDB to connect to  | `timescale`                        |
| `connection.sslMode`              | SSL mode for connection                     | `require`                          |
| `service.metricsPort`             | Port the connector pods will accept metrics connections on | `9201`                      |
| `service.tracesPort`              | Port the connector pods will accept traces connections on | `9202`                      |
| `service.loadBalancer.enabled`    | If enabled will create an LB for the connector, ClusterIP otherwise | `true`     |
| `service.loadBalancer.annotations`| Annotations to set to the LB service        | `service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout: "4000"` |
| `serviceMonitor.enabled`          | Enable creation of serviceMonitor object used by prometheus-operator. This should be used with `prometheus.enabled: false`. | `false`   |
| `maintenance.enabled`             | Option to enable maintenance cronjob, Enable maintenance cronjob only if you are using TimescaleDB < `2.0`   | `false` |
| `maintenance.schedule`            | The schedule with which the Job, that deletes data outside the retention period, runs | `0,30 * * * *` |
| `maintenance.startingDeadlineSeconds` | If set, CronJob controller counts how many missed jobs occurred from the set value until now | `200` |
| `maintenance.successfulJobsHistoryLimit` | The number of successful maintenance pods to retain in-cluster | `3`      |
| `maintenance.failedJobsHistoryLimit` | The number of failed maintenance pods to retain in-cluster | `1`              |
| `maintenance.resources` | Requests and limits for maintenance cronjob | `{}`              |
| `maintenance.nodeSelector`                    | Node labels to use for scheduling maintenance cronjob          | `{}`                               |
| `maintenance.tolerations`                     | Tolerations to use for scheduling maintenance cronjob          | `[]`                               |
| `maintenance.affinity`                        | PodAffinity and PodAntiAffinity for scheduling maintenance cronjob          | `{}`                               |
| `resources`                       | Requests and limits for each of the pods    | `{}`                               |
| `nodeSelector`                    | Node labels to use for scheduling           | `{}`                               |
| `tolerations`                     | Tolerations to use for scheduling           | `[]`                               |
| `affinity`                        | PodAffinity and PodAntiAffinity for scheduling           | `{}`                               |
| `tracing.enabled`                 | Enable tracing support in Promscale, exposes container, service ports to default 9202 (in future releases tracing support will be enabled by default)           | `false`                               |
| `tracing.args`                    | Promscale args to enable tracing               | `-otlp-grpc-server-listen-address=:9202`                               |

[docker-image]: https://hub.docker.com/timescale/promscale