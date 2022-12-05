# Epoch-based series cache invalidation

This document describes the mechanism used to maintain the series cache in
Promscale.

## Background

Each time series in Prometheus is uniquely identified by the set of labels
associated with it. In Promscale, we create an entry in the
`_prom_catalog.series` table for each series. Thus, each series has a unique
identifier in the database.

The Promscale series cache contains a lookup from series labels to series id.
It is heavily used while ingesting data, and drastically increases ingest
throughput (from ~10k samples/sec to ~400k samples/sec).

Rows in the `_prom_catalog.series` table can be removed. This happens when all
data belonging to a series has been removed (e.g. through the application of
data retention policies, or manual deletion). The process of deleting a series
takes place in the database, a process over which the Promscale connector has
no control. It is possible for a series to be recreated once deleted. When this
happens, the series id is not the same as before.

To improve insertion performance, there is no foreign key between the data
table, and the `_prom_catalog.series` table. Inserting samples in the data
table with the incorrect series id will result in inconsistent data in the
database.

This implies that we require a mechanism to ensure that entries in the series
cache are invalidated when the underlying row is removed from the database.
This is the cache invalidation mechanism.

## Soft-deletion of series

In order to allow time for the synchronisation of deleted series rows between
the database and the Promscale connector, we soft-delete a series in the
database. Once the series row has been soft-deleted for long enough, it is
removed. "Long enough" is configured by the `epoch_duration` parameter, which
is 12 hours by default.

In the database, we track two unix timestamps in the `_prom_catalog.ids_epoch`
table: `current_epoch`, and `delete_epoch`.
`current_epoch` contains the current timestamp (in seconds since the unix epoch)
at the most recent update.
`delete_epoch` contains a timestamp marking which rows were removed (this will
become clear shortly).
Note: using `current_epoch` as the fixed notion of when "now" is gets around
issues of time synchronisation between different systems.

The soft-deletion works as follows: When a row is to be removed, we set the
`delete_epoch` column of `_prom_catalog.series` to the value currently
contained in the `current_epoch` column of the `_prom_catalog.ids_epoch` table.
This indicates: it was decided at `current_epoch` to delete this row at some
point in the future.

Periodically, `_prom_catalog.ids_epoch.delete_epoch` is set to
`current_epoch` - `epoch_duration`, after which all rows matching the following
condition are removed:
`prom_catalog.series.delete_epoch` >= `prom_catalog.ids_epoch.delete_epoch`.

## Cache refresh

The series cache is refreshed by periodically fetching all series rows which
are marked for deletion, and evicting them from the series cache.

## Insert abort

There is one final mechanism at play, which ensures that stale data is not
accidentally inserted: insert abort.

The idea behind insert abort is to ensure that if a stale/outdated cache was
used to insert data, the insertion will not succeed.

The mechanism used to achieve this is to keep track of the `current_epoch`
value at which the cache was populated. When the cache is refreshed, the epoch
is advanced to the value of `current_epoch` at refresh time.

When inserting data, we assert that value of the Promscale connector's
`current_epoch` which was used to look up all series ids is strictly less than
the database's `delete_epoch`. This ensures that we did not use a stale cache
to populate series ids.

## References

There are other artifacts which may be useful for reference.

The [cache invalidation epic][epic-cache-invalidation] document contains an
analysis of the situation before invalidation was introduced. There were a
number of different proposals for how it could be implemented. A full analysis
with pro/contra arguments for the different approaches is covered there.

There are two documents which serve as a proof that the invalidation mechanism
is sound (in the presence of long-running transactions). Investigating them may
give a better insights into the specifics of hat has been implemented than the
prose description above. The first is a [manual proof][manual-cache-proof], the
second is a [TLA+ model][tlaplus-cache-proof] of the cache invalidation system.

[epic-cache-invalidation]: https://docs.google.com/document/d/1PtUnz7zhOpDgmURmoDWdubnlRQTnp1VaVyWEBsGr44k/edit
[manual-cache-proof]: https://docs.google.com/document/d/1U4brZ4rZcn3PAv7hdot_r1SXeP4NGY8Im1d4NDJkZlU/edit
[tlaplus-cache-proof]: ../../specs/cache.tla

