# Prerequisites 

The chart expects that the password used to connect to TimescaleDB is
created before the chart is deployed.

# What the chart deploys

The chart will create:
    - A Deployment for the prometheus-connector
      - number of pods spawned can be controlled with `replicaCount`
    - A Service for the adapter
      - If `service.loadBalancer.enabled` is `true` an annotated LoadBalancer is created
      - Otherwise a headless ClusterIP
      - The port of the exposed service is controlled by `service.port`

Upon installation the adapter will attempt to connect to the database (restarting itself if the db is not ready) 
and execute the migration scripts.

# Example configurations

1. Connecting to an already deployed instance of TimescaleDB

```
connection:
  passwordSecret: "name-of-secret-containing-the-password"
  host:
    nameTemplate: "service-of-db-created-in-the-other-release"
```

2. Deploying this chart in the same release with TimescaleDB

``` 
connection:
  passwordSecret: "name-of-secret-containing-the-password"
  # no modification needed if no overrides specified for TimescaleDB deployment
  # given here as example
  host:
    nameTemplate: "{{ .Release.Name }}.default.svc.cluster.local"
```