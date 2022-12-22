# PromscalePostgreSQLSharedBuffersLow

## Meaning

Open chunks (the chunks where data is current written into) can't fit into
PostgreSQL shared buffers. Total size is calculated by summing up all chunk
relations and indexes sizes.

## Impact

Database performance will be affected, especially the ingest speed. The effect
will be less if you are running PostgreSQL on fast local disk.

## Mitigation

Increase percentage of PostgreSQL memory allocated to shared buffers.
If you have already allocated huge percentage of memory to shared buffers (eg 75%)
you should consider increasing database memory.
`shared_buffers` can be set through `postgresql.conf`.
To make sure that your new setting is applied you can run: 
`SELECT * FROM pg_settings WHERE name = 'shared_buffers';` 
