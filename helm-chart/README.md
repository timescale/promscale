The chart will create:
    - A Deployment for the prometheus-connector
      - number of pods spawned can be controlled with `replicaCount`
    - A LoadBalancer Service for the adapter
    - If `password.create` is set to `true` in the values
        - A secret named `password.secretName` will be created
        - It will contain the password provided in `password.value` indexed by the user
        provided in `user` 
    - Else if `password.create` is set to `false`
        - `password.secretName` needs to be set to the identifier of an existing secret
        (such as the one created by an already deployed TimescaleDB instance)
        - the key used to query the secret will be the value provided in `user`

Upon installation the adapter will attempt to connect to the database (restarting itself if the db is not ready) 
and execute the migration scripts.
