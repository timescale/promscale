# SQL Permissions

Promscale adopts a Role Based Access Control (RBAC) philosophy to managing permissions
at the SQL layer. Many users will choose to simply use a single PostgreSQL user for all
Promscale operations (using a single owner user as defined belows).
But we also allow users more granular control over permissions management.

Promscale defines several roles:
- **Owner** this is the owner of the Promscale schema and objects. It is defined
  as the user that originally installed/migrated Promscale. This user needs to be
  able to install several PostgreSQL extensions and thus either needs to be a
  PostgreSQL superuser or use [pgextwlist](https://github.com/dimitri/pgextwlist) to authorize
  a non-superuser to install the extensions(see below). The owner should be consistent
  every time you migrate the Promscale schema.
- **prom_reader** this role is allowed to read but not modify the data stored by Promscale.
- **prom_writer** is allowed to read Promscale data as well as write (insert) new data. This
  role is not allowed to modify or delete data. Includes all permissions of prom_reader.
- **prom_modifier** is able to read, write, and modify all Promscale data. Includes all permissions of prom_writer.
- **prom_admin** is allowed to change the configuration options associated with Promscale. This
  includes data retention policies, chunk intervals, etc. Includes all permissions of prom_writer.
- **prom_maintenance** is allowed to execute maintenance tasks such as compression and data retention jobs.
  mostly only used externally unless using a CRON job to execute_maintenance() instead of the jobs framework.
  Includes all permissions of prom_reader.

You can assign roles to users with the following sql command:
`GRANT <role> to <user>`. A user can be assigned multiple roles. More information
can be found in the [PostgreSQL docs about permissions](https://www.postgresql.org/docs/current/user-manag.html).


Promscale can be run by the owner or a user with the `prom_modifier`, `prom_writer` or `prom_reader` role.
The following table describes what features of promscale are available with each role:

|user / role| schema migration | delete series api | write api | read api |
| --- | --- | --- | --- | --- |
| owner | ✅ | ✅ | ✅ | ✅|
| prom_modifier | ❌ | ✅ | ✅ | ✅|
| prom_writer | ❌ | ❌ | ✅ | ✅|
| prom_reader | ❌ | ❌ | ❌ | ✅|


# Examples

## Simplest possible deployment with one owner

To use a simple deployment with a single owner `tsdbadmin` that has permissions to
install the Promscale and Timescaledb extensions simply start Promscale with:

`PGPASSWORD=<password> ./promscale -db-uri=postgres://tsdbadmin@<hostname>:<port>/<databasename>?sslmode=require`

## Deployments with separate owner and modifier users

If you want to more tightly control your permissions and want to separate out
the users that are used for normal Promscale operations from the users that
can upgrade the Promscale schema do the following. Given a database user `tsdbadmin`
that has permissions to install the Promscale and Timescaledb extensions and will
become the Promscale owner.

First, install the Promscale schema:

`PGPASSWORD=<password> ./promscale -db-uri="postgres://tsdbadmin@<hostname>:<port>/<databasename>?sslmode=require" -migrate=only`

This will exit as soon as the schema is installed thanks to the `-migrate=only` flag.

Next, create a more limited user for use by Promscale by connecting with psql:
`PGPASSWORD=<password> psql "postgres://tsdbadmin@<hostname>:<port>/<databasename>?sslmode=require"`

and executing:

```sql
   CREATE ROLE promscale_modifier_user PASSWORD '<new password>' LOGIN;
   GRANT prom_modifier TO promscale_modifier_user;
```

Now, start promscale with that user:
`PGPASSWORD=<password> ./promscale -db-uri="postgres://promscale_modifier_user@<hostname>:<port>/<databasename>?sslmode=require"  -install-extensions=false -migrate=false -upgrade-extensions=false`


# Using a non-superuser owner with pgextwlist

The owner user of Promscale need to be able to install
and upgrade the promscale and timescaledb PostgreSQL extensions.

Normally extension installation and upgrade requires superuser permissions. To avoid
using superuser, you could use the [pgextwlist](https://github.com/dimitri/pgextwlist) extension with the following PostgreSQL
config:
```
local_preload_libraries=pgextwlist
extwlist.extensions=promscale,timescaledb
```