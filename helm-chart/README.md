The chart will create:
    - A pod for the prometheus-postgresql-adapter
    - A LoadBalancer Service for the adapter
    - If `timescaledb-single.enabled` is set to `true` in `values.yaml`
        - A timescaledb statefull set with replication 
        - Replication can be controlled with `timescaledb-single.replicaCount`
    - Else if `timescaledb-single.enabled` is set to `false`
        - `promadapter.user` `promadapter.pass` and `promadapter.host` need to be set in order for the adapter to establish a connection to TS
        - the password will need to be base64 encoded because it's stored in a secret

Upon installation the adapter will attempt to connect to the database (restarting itself if the db is not ready) and if the metrics tables are missing it will create them.
