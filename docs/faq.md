# F.A.Q.
1. What happened to prometheus-postgresql-adapter/pg_prometheus?

   Promscale and [Promscale extension](https://github.com/timescale/promscale_extension) are replacing those products with improved features and performance.

2. Does Promscale work with vanilla PostgreSQL?

   Yes, Promscale works with PostgreSQL without the TimescaleDB extension installed. However, we suggest running it with TimescaleDB to improve the general performance and reduce storage size using the compression feature included with TimescaleDB.

3. Is Promscale compatible with Kubernetes?

   Yes. Promscale has first-class Kubernetes support. You can actually use the tobs tool to easily include it in your observability stack: https://github.com/timescale/tobs ([demo video](https://www.youtube.com/watch?v=MSvBsXOI1ks))

4. Can I run PromQL queries directly on Promscale?

   Yes. Promscale implements the [HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/) from Prometheus to support the full PromQL query interface. You can use it as a Prometheus datasource in 3rd party tools like Grafana.

5. How do I query the data using SQL?

   There is a document describing the SQL schema and API that Promscale exposes [here](https://github.com/timescale/promscale/blob/master/docs/sql_schema.md).

6. How do I delete data?

   Right now, there are two ways of deleting ingested data:
   - Automatically using the data retention properties ([details here](https://github.com/timescale/promscale/blob/master/docs/sql_schema.md#data-retention))
   - Manually through SQL interface (either Postgres [truncate table](https://www.postgresql.org/docs/current/sql-truncate.html) or [drop_chunks](https://docs.timescale.com/latest/api#drop_chunks) TimescaleDB function)

7. What is the best way to set up HA (high availability) on Promscale?

   You can find the documentation on Promscale HA setup [here](https://github.com/timescale/promscale/blob/master/docs/high-availability/prometheus-HA.md).

8. What is this Promscale extension for?

   Promscale extension is an optional additional extension separate from TimescaleDB that can be used to improve the general performance characteristics of Promscale. You can find more info here: https://github.com/timescale/promscale_extension. Note the CREATE EXTENSION command will be executed automatically by Promscale when installing.

9. Does Promscale support backfill?

   Yes. Promscale support backfill of old data by sending older date to Promscale using the Prometheus [remote write API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations). This requires creating a custom application that can send requests to that API. We have plans in the near future to expose an SQL API to ingest data into Promscale.

10. How do I store existing data from Prometheus to Promscale?

    For migrating data from Prometheus to Promscale, you can use Prom-migrator. For more information on Prom-migrator, please read the [docs](https://github.com/timescale/promscale/tree/master/migration-tool/cmd/prom-migrator).

11. Why doesn’t Prometheus show Promscale data when I use the PromQL dashboard?

    If you have backfilled data which is not ingested via Prometheus itself, you will need to enable the `read_recent` option for Promscale remote read endpoint ([details here](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read)).

12. How do I make the maximum use of Promscale in terms of efficient storage use?

    Since all your Prometheus data will be stored in Promscale as well, we suggest keeping the Prometheus retention period to 6 hours so your Prometheus instance will not take up unnecessary storage space.

13. Are recording rules supported by Promscale?

    Yes. Prometheus manages the rules and Promscale will create the series on command from Prometheus.

14. How can I find out more about Promscale?

    If you have any questions, please join the #prometheus channel on [TimescaleDB Slack](https://slack.timescale.com/). You're also welcome to schedule a 1-to-1 with members of the Promscale team using ([our office hours calendar](https://calendly.com/promscale)).

15. Does Promscale need a database superuser to run?

    No, Promscale does not need to run as superuser when ingesting or querying data. To setup or update the schema, you could run the Promscale with the `-migrate=only` flag using a superuser which will install the update the schema and install the necessary extensions and quit. You can also avoid needing superuser access by installing the TimescaleDB extension manually before starting the connector.

16. How do I downsample data with Promscale?

    Right now, the easiest way is to use [Prometheus recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) to downsample the wanted data and set a longer evaluation interval for the recording rule group. Note that recording rules only allow you to downsample data after the rule has been created and, as of right now, it cannot be applied to pre-existing data.

17. Why am I getting `out of shared memory` error when using pg_dump/pg_restore to backup/restore Promscale data?

    Promscale creates a table for each metric. Depending on your setup, that can be a lot of tables. pg_dump/pg_restore needs to lock each table when working on it so they can require a lot of locks in a single transaction. Increasing the `max_locks_per_transaction` setting in PostgreSQL should help in this situation.

18. Why is my data occupying so much space?

    Promscale keeps metric data in hypertables, which consists of both compressed and not-compressed chunks. The most recent chunk kept uncompressed as a cache for faster querying. As data ages, it becomes compressed. Thus our data usage consists of both an uncompressed cache and compressed data. Thus, we have a constant-sized overhead for the cache compared to other systems, in return for faster query results.

    To check that compression is working correctly you can query the `prom_info.metric` view and make sure that `total_chunks-number_compressed_chunks` is not bigger than 2. If compression is not working correctly:
    - Make sure that you have sufficient background workers (i.e., > number of databases + 2) that is required to do scheduled jobs like compression and retention.
    - If you are using TimescaleDB version less than 2.0.0, make sure that you are running the maintenance cron jobs, and they are returning success.
