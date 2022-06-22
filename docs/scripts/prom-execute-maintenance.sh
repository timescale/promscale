#!/bin/sh
export PGPASSWORD="<PASSWORD>"
psql --host localhost --port 5432 --dbname postgres --user postgres -c 'CALL prom_api.execute_maintenance();'