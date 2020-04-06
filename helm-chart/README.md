# Timescale Prometheus Helm chart

This directory contains a Helm chart to deploy the Timescale Prometheus Connector
on Kubernetes. This chart will do the following:

* Create a Kubernetes Deployment (by default) with one pod
  * The pod has a container created using the [Timescale Prometheus Connector Docker Image][docker-image] 
* Create a Kubernetes Service exposing access to the Connector pods
  * By default a LoadBalancer, but can be disabled to only a ClusterIP with a configurable port
* Create a Kubernetes CronJob that deletes the data chunks that fall out of the retention period 

## Prerequisites 

The chart expects that the password used to connect to TimescaleDB is
created before the chart is deployed. 
You can set the secret name by modifying the  `password.secretTemplate` value. 
Templating is supported and you can use:
```yaml
password:
  secretTemplate: "{{ .Release.Name }}-timescaledb-passwords"
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

Alternatively, a YAML file the specifies the values for the parameters can be provided
while installing the chart. For example:
```shell script
helm install --name my-release -f myvalues.yaml .
```

## Configuration

|       Parameter                   |           Description                       |               Default              |
|-----------------------------------|---------------------------------------------|------------------------------------|
| `image`                           | The image (with tag) to pull                | `timescale/timescale-prometheus`   |
| `replicaCount`                    | Number of pods for the connector            | `1`                                |
| `connection.user`                 | Username to connect to TimescaleDB with     | `postgres`                         |
| `password.secretTemplate`         | The template for generating the name of a secret object which will hold the db password | `{{ .Release.Name }}-timescaledb-passwords` |
| `host.nameTemplate`               | The template for generating the hostname of the db | `{{ .Release.Name }}.default.svc.cluster.local` |
| `port`                            | Port the db listens to                      | `5432`                             |
| `service.port`                    | Port the connector pods will accept connections on | `9201`                      |
| `service.loadBalancer.enabled`    | If enabled will create an LB for the connector, ClusterIP otherwise | `true`     |
| `service.loadBalancer.annotations`| Annotations to set to the LB service        | `service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout: "4000"` |
| `dropChunk.schedule`              | The schedule with which the drop-chunk Job runs | `0,30 * * * *`                 |
| `resources`                       | Requests and limits for each of the pods    | `{}`                               |
| `nodeSelector`                    | Node labels to use for scheduling           | `{}`                               |

[docker-image]: https://hub.docker.com/timescale/timescale-prometheus