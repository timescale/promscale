# Running Promscale in High-Availability (HA) mode

Promscale is a stateless service, thus it can run safely with multiple
replicas. A load-balancer can simply route Promscale requests to any replica.

# Using Promscale with Prometheus deployed in High Availability (HA) mode

Promscale supports running Prometheus in High Availability (HA). This means
that you can have multiple Prometheus instances running as "HA
pairs". Such "HA pairs" (what we will call clusters from now on) are simply two
or more identical Prometheus instances running on different
machines/containers. These instances scrape the same targets and should thus
have very similar data (the differences occur because scrapes happen at slightly
different times). You can think of a **cluster** as a group of similar
Prometheus instances that are all scraping the same targets and **replicas**
as individual Prometheus instance in that group.

Promscale supports de-duplicating data coming from such HA clusters upon
ingest. It does this by electing a leader Prometheus instance and only
ingesting data from that leader. If the leader Prometheus stops sending data
to Promscale for any reason, Promscale will fail-over to the other Prometheus
instance.

Promscale supports having multiple clusters sending data to the same set of
Promscale instances. It will elect one leader per cluster.

## Prometheus leader-election via external labels

![Promscale architecture for Prometheus in HA using external labels](https://raw.githubusercontent.com/timescale/promscale/master/docs/high-availability/new_ha_system.png)

To set up Promscale to process data from Prometheus running in HA mode,
Prometheus must be configured to communicate which cluster it belongs to
using external labels. In particular, each Prometheus should send a `cluster`
label to indicate which cluster it's in and a `__replica__` label that
provides a unique identifier for the Prometheus instance with the cluster.
Two Prometheus instances running as part of a HA cluster, MUST send the same
`cluster` label and different `__replica__` labels.

In Kubernetes environments, it is often useful to set the `__replica__`
label to the pod name, and the cluster name as the Prometheus deployment/statefulset name. When using the [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator#prometheus-operator) this can be done
with the following settings:

```
  replicaExternalLabelName: "__replica__"
  prometheusExternalLabelName: "cluster"
```

In bare metal and other environments, you need to configure external labels with the cluster and replica names in each Prometheus instance config as mentioned below:

```
global:
  external_labels:
    __replica__: <REPLICA_NAME> (This should be unique name of the Prometheus instance)
    cluster: <CLUSTER_NAME> (This should be the name of the Prometheus deployment, which should be common across the Prometheus replica instances.)
```

After Prometheus instances are configured to send the correct labels,
Promscale simply needs to be started with the `-metrics.high-availability` CLI flag.
Internally, Promscale will elect a single replica per cluster to be the
current leader. Only data sent from that replica will be ingested. If that
leader-replica stops sending data, then a new replica will be elected as the
leader.


## Leader-election method via pg_advisory_lock (will be deprecated in the future)

Note: This method is legacy and _not_ recommended for new deployments.

PostgreSQL provides a [pg_advisory_lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS) that locks a application-defined resource based on an ID. Promscale makes use of this lock when running multiple Promescale instances in parallel in order to choose one leader.

![Promscale architecture for Prometheus in HA using pg_advisory_locks](https://raw.githubusercontent.com/timescale/promscale/master/docs/high-availability/old_ha_system.png)

When you want to run Prometheus in HA mode, you need to run Promscale in parallel to the Prometheus instance as an one-to-one relation. This means that each Prometheus instance will have its own Promscale connector which will write into the database. Each cluster of Promscale instances should be identified by a unique advisory-lock-id. Given a set of Promscale instances with the same id, only one will be chosen  as a leader and will actually write data into TimescaleDB. The non-leaders will be on standby, waiting to take over in case the leader fails. The end result is that data from exactly one Prometheus instance ends up being written into the database.

Promscale provides a flag **leader-election-pg-advisory-lock-id** to set the advisory lock ID. Please note that the ID provided in this flag should be the same across Prometheus HA pairs. This means that different groups of decoupled HA pairs (i.e. different clusters) can run in parallel, with each cluster’s lock-id being different. The leader-election-lock-id flag should be accompanied by a timeout flag **leader-election-pg-advisory-lock-prometheus-timeout** which is a duration input. This flag is used to check the liveness of a Prometheus instance. By default, we assume that there are only two Promscale instances in a cluster, if this is not the case you should set **db-connections-max** explicitly on each Promscale instance so that the sum does not exceed the maximum number of connections allowed by the database.

We store the last active time of a Prometheus instance when we receive a write request. The difference between now() and the last active time gives us the elapsed duration of activeness. If this duration is greater than the timeout specified, we conclude that the Prometheus instance in the current pair is dead. Hence, the Promscale connector (in the current HA pair) resigns from being a leader. This enables one of the other Promscale instances in the cluster to become the leader, enabling them to take over writing data to the database.

**Important:** We need to note that the liveness of Prometheus is checked in the intervals of 10 seconds. This means that the maximum possible (worst case) data loss when shifting the leader should not be greater than 10 seconds. Therefore, if you have flush_duration of 10 seconds (which generally is the case for the slowest flush provided you have new samples in the prometheus queue), you can lose 2 scrapes, one just after the current livecheck and the other just before the following/upcoming livecheck (since go tickers have an error range of +- 0.2 secs).

_In future versions, we plan to introduce a buffer to fix this issue. The buffer will hold samples from the non-leader promscale instance up to then twice the livecheck calculated above._
