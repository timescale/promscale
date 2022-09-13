# Vacuum Engine

The Promscale connector has a background job called the vacuum engine.
The goal of the vacuum engine is to help prevent transaction id wraparound
which can be a problem for high transaction workloads.
It periodically looks for compressed chunks that likely need to be
[vacuumed/frozen](https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND).
These chunks are manually [vacuumed](https://www.postgresql.org/docs/current/sql-vacuum.html)
with the freeze and analyze options.

Only one instance of the engine needs to run at a time across all Promscale
connector instances. An advisory lock is held in the database while the engine
is running. This lock is used to coordinate and prevent concurrent runs.

If the engine finds chunks to vacuum, it can use multiple database connections
to parallelize the work. Multiple chunks will be vacuumed simultaneously. The
degree of parallelism can be configured.
