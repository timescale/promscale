# Promscale Helm chart

This directory contains a Helm chart to deploy the Promscale Connector on Kubernetes.
If you are looking to deploy an entire observability suite including Prometheus,
Promscale, Grafana and other tools, we suggest using
[The Observability Suite for Kubernetes (tobs)][https://github.com/timescale/tobs].

This chart will do the following:

* Create a Kubernetes Deployment (by default) with one pod
  * The pod has a container created using the [Promscale Connector Docker Image][docker-image]
* Create a Kubernetes Service exposing access to the Connector pods
  * By default a LoadBalancer, but can be disabled to only a ClusterIP with a configurable port
* Create a Kubernetes CronJob that deletes the data chunks that fall out of the retention period

## Prerequisites

### Database Name

The name of the database the Promscale connector will connect to by default
is set to `timescale`. You can change it by modifying the `connection.dbName` value.
The database **must be created before** starting the connector.

### Password

The chart expects that the password used to connect to TimescaleDB is stored in a
Kubernetes Secret created before the chart is deployed.
You can set the secret name by modifying the  `connection.password.secretTemplate` value.
Templating is supported and you can use:
```yaml
connection:
  password:
    secretTemplate: "{{ .Release.Name }}-timescaledb-passwords"
```

The data in the Secret object should look like this:
```yaml
data:
  username: base64encodedPassword
```
where `username` is the user that the Connector will use to connect to the
database. By default the *'postgres'* user is used, as set in `.Values.connection.user`.

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

Alternatively, a YAML file the specifies the values for the parameters can be provided
while installing the chart. For example:
```shell script
helm install --name my-release -f myvalues.yaml .
```

## Configuration

|       Parameter                   |           Description                       |               Default              |
|-----------------------------------|---------------------------------------------|------------------------------------|
| `image`                           | The image (with tag) to pull                | `timescale/promscale`   |
| `replicaCount`                    | Number of pods for the connector            | `1`                                |
| `connection.user`                 | Username to connect to TimescaleDB with     | `postgres`                         |
| `connection.password.secretTemplate`| The template for generating the name of a secret object which will hold the db password | `{{ .Release.Name }}-timescaledb-passwords` |
| `connection.host.nameTemplate`    | The template for generating the hostname of the db | `{{ .Release.Name }}.{{ .Release.Namespace}}.svc.cluster.local` |
| `connection.port`                 | Port the db listens to                      | `5432`                             |
| `connection.dbName`               | Database name in TimescaleDB to connect to  | `timescale`                        |
| `connection.sslMode`              | SSL mode for connection                     | `require`                          |
| `service.port`                    | Port the connector pods will accept connections on | `9201`                      |
| `service.loadBalancer.enabled`    | If enabled will create an LB for the connector, ClusterIP otherwise | `true`     |
| `service.loadBalancer.annotations`| Annotations to set to the LB service        | `service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout: "4000"` |
| `dropChunk.schedule`              | The schedule with which the drop-chunk Job runs | `0,30 * * * *`                 |
| `resources`                       | Requests and limits for each of the pods    | `{}`                               |
| `nodeSelector`                    | Node labels to use for scheduling           | `{}`                               |

[docker-image]: https://hub.docker.com/timescale/promscale