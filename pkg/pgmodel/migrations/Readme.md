# Version Migrations  #

This directory contains sql scripts for schema setup and upgrading.

All scripts are contained in the `sql` directory and are separated as follows:

1. `preinstall` - This directory contains all scripts that will be executed on 
    a new database install.
2. `idempotent` - This directory contains all scripts that contain idempotent 
    content which is executed after a fresh install or a version upgrade.
3. `versions` - This directory contains subdirectories that are named after 
    the app version they were introduced in. Their names follow the semantic versioning syntax.

All script files are executed in a explicit order. Ordering can happen in two ways:

- Using a table of contents which needs to be present in the `pkg/pgmodel/migrate.go` 
    file. Files not present in the ToC will be ignored.
- Using a numbered prefix delimited by a `-` e.g. `1-base.sql`, `2-secondary.sql`. 
    In this case, fils are ordered by the numeric prefix, low to high. 
    Files that do not follow this format will be ignored.

**NOTE** If any changes are made to the `sql` directory, you must rerun
         `go generate` in this directory.
