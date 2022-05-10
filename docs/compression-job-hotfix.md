# Compression Job Hotfix to Improve Performance

Users have reported that Promscale background maintenance jobs that handle data
compression and retention get slower over time as the number of TimescaleDB
chunks grows. We've identified the problem which is related to two suboptimal
queries used when compressing data. The code that uses this queries are
functions that run inside the database and so the hotfix involves updating
those functions by executing SQL commands against the database. This problem
affects Promscale versions 0.10.0 and earlier.

Below we explain the steps to apply a hotfix to your Promscale instance. This
fix will be included in the next release of Promscale.

## Steps to Apply the Hotfix

1. Upgrade Promscale to version 0.10.0.
2. Download the [hotfix file](scripts/hotfix-compression-job-performance.sql).
3. Apply the hotfix by executing the SQL code in the hotfix. For example you can
   use psql:

   `psql <your connection options> -f hotfix-compression-job-performance.sql`

## Rollback the Hotfix

To rollback the hotfix and restore the original version of the functions in
Promscale 0.10.0:

1. Download the [rollback file](scripts/rollback-hotfix-compression-job-performance.sql).
2. Apply the rollback file by executing the SQL code in the hotfix:

   `psql <your connection options> -f rollback-hotfix-compression-job-performance.sql`
