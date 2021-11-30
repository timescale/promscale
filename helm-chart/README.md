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

**Note:** We have dropped compatibility with TimescaleDB 1.x. If you would like to use Promscale with TimescaleDB 1.x then use the helm charts from Promscale release 0.6.0. 

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
  host: timescaledb.default.svc.cluster.local
  port: 5432
  sslMode: require
  dbName: timescale
```

### User-Managed Connection Parameter Secret

**Note:** We primarily provide this feature for workflow convenience, but it is an advanced feature requiring more care to configure correctly. We suggest that you configure the connection options in `values.yaml` instead. If you are using GitOps and do not want to store database passwords unencrypted in your Git repo, you probably want to use tooling like [helm-secrets](https://github.com/jkroepke/helm-secrets) instead.

If you would like to provide connections details via your own secret which is managed outside the lifecycle of this helm chart, you can do so. The secret must contain the environment variables corresponding to Promscale's connection configuration. Additionally, the `connectionSecretName` parameter of this helm chart must be configured with the name of your secret, and your secret must be in the same namespace as the deployment.

An example of what this could look like is as follows:
```yaml
# secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-promscale-connection-secret
stringData:
  PROMSCALE_DB_PORT: "5432"
  PROMSCALE_DB_USER: "postgres"
  PROMSCALE_DB_PASSWORD: "password"
  PROMSCALE_DB_HOST: "db.timescale.svc.cluster.local"
  PROMSCALE_DB_NAME: "timescale"
  PROMSCALE_DB_SSL_MODE: "require"
```

In the values.yml of the helm chart you would then have the following:

```yaml
# extract from values.yaml
connectionSecretName: my-promscale-connection-secret
```

**Note:** The Promscale pod, once started, will not automatically take on any changes to the contents of `connectionSecretName`.

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
      --set connectionSecretName="promscale-connection-secret"
```

You can also install by referencing the db uri secret created previously:

```shell script
helm install --name my-release . \
      --set connectionSecretName="promscale-connection-secret"
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
| `resources`                       | Requests and limits for each of the pods    | `{}`                               |
| `nodeSelector`                    | Node labels to use for scheduling           | `{}`                               |
| `tolerations`                     | Tolerations to use for scheduling           | `[]`                               |
| `affinity`                        | PodAffinity and PodAntiAffinity for scheduling           | `{}`                               |
| `tracing.enabled`                 | Enable tracing support in Promscale, exposes container, service ports to default 9202 (in future releases tracing support will be enabled by default)           | `false`                               |
| `tracing.args`                    | Promscale args to enable tracing               | `-otlp-grpc-server-listen-address=:9202`                               |

[docker-image]: https://hub.docker.com/timescale/promscale