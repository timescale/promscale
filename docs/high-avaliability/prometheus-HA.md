# Using Promscale with Prometheus in High Availability

Promscale supports running Prometheus in High Availability (HA). This means that more than one Prometheus instances can run in parallel. Promscale implements leader election to support Prometheus HA mode through pg_advisory_lock.

## Leader-election method via pg_advisory_lock

PostgreSQL provides a [pg_advisory_lock](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS) that locks a application-defined resource based on an ID. Promscale makes use of this lock when running multiple promescale instances in parallel in order to choose one leader.

![Promscale architecture for Prometheus in HA](https://github.com/Harkishen-Singh/promscale/blob/docs_HA_promscale/docs/assets/promscale-HA-arch.png?raw=true)

When you want to run Prometheus in HA mode, you need to run Promscale in parallel to the Prometheus instance as an one-to-one relation. This means that each Prometheus instance will have its own Promscale connector which will write into the database. Each cluster of Promscale instances should be identified by a unique advisory-lock-id. Given a set of Promscale instances with the same id, only one will be chosen  as a leader and will actually write data into TimescaleDB. The non-leaders will be on standby, waiting to take over in case the leader fails. The end result is that data from exactly one Prometheus instance ends up being written into the database.

Promscale provides a flag **leader-election-pg-advisory-lock-id** to set the advisory lock ID. Please note that the ID provided in this flag should be the same across Prometheus HA pairs. This means that different groups of decoupled HA pairs (i.e. different clusters) can run in parallel, with each cluster’s lock-id being different. The leader-election-lock-id flag should be accompanied by a timeout flag **leader-election-pg-advisory-lock-prometheus-timeout** which is a duration input. This flag is used to check the liveness of a Prometheus instance.

We store the last active time of a Prometheus instance when we receive a write request. The difference between now() and the last active time gives us the elapsed duration of activeness. If this duration is greater than the timeout specified, we conclude that the Prometheus instance in the current pair is dead. Hence, the Promscale connector (in the current HA pair) resigns from being a leader. This enables one of the other Promscale instances in the cluster to become the leader, enabling them to take over writing data to the database.

**Important:** We need to note that the liveness of Prometheus is checked in the intervals of 10 seconds. This means that the maximum possible (worst case) data loss when shifting the leader should not be greater than 10 seconds. Therefore, if you have flush_duration of 10 seconds (which generally is the case for the slowest flush provided you have new samples in the prometheus queue), you can lose 2 scrapes, one just after the current livecheck and the other just before the following/upcoming livecheck (since go tickers have an error range of +- 0.2 secs).

_In future versions, we plan to introduce a buffer to fix this issue. The buffer will hold samples from the non-leader promscale instance up to then twice the livecheck calculated above._
