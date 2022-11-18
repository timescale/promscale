# Vacuum Engine

The Promscale connector has a background job called the vacuum engine.
The goal of the vacuum engine is to help prevent transaction id wraparound
which can be a problem for high transaction workloads.

Once a chunk is compressed, it is unlikely to be modified.
Therefore, a compressed chunk should only need to be vacuumed once
and never again. We want to vacuum a compressed chunk as soon as
possible in hopes of freezing all the pages for the chunk. We want
to freeze so that we don't risk burning through transaction ids too
quickly under heavy ingest.

The vacuum engine periodically wakes up. If it can grab an advisory
lock (only one vacuum engine runs at a time per database regardless
of the number of connectors), it will look for compressed chunks
that are not fully frozen.

If chunks needing freezing are found, it uses the maximum
transaction age of the chunks in the set to determine how many
workers to use to vacuum the chunks. The closer the maximum
transaction age is to autovacuum_freeze_max_age the more workers are
used (up to a maximum number). This is done to throttle how much
database CPU is consumed by vacuuming.

We ignore chunks that have been vacuumed in the last 15 minutes.

We ignore chunks with a transaction id age less than
vacuum_freeze_min_age.

We grab a list of chunks and then let workers pull from this list.
Vacuuming may take a while, so there is a decent chance that the
postgres autovacuum engine may vacuum a chunk before we get to it.
We get the autovacuum count when we produce the list. Just before
vacuuming a chunk, we check the autovacuum count again to see if it
has increased. If it has, the autovacuum engine beat us to the
chunk, and we skip it.
