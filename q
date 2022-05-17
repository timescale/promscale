[33mcommit ca3bec723f3560f80dc5afb5c0da0c784eff1bd1[m[33m ([m[1;36mHEAD -> [m[1;32mjs/rules-alerts-alertmanager[m[33m)[m
Merge: 4e7089fb 749cf0d3
Author: James Sewell <james.sewell@gmail.com>
Date:   Tue May 17 15:45:48 2022 +0200

    Merge branch 'master' into js/rules-alerts-alertmanager

[33mcommit 4e7089fb4f15f1cbea4b4ccbb48243b608cffa56[m[33m ([m[1;31morigin/js/rules-alerts-alertmanager[m[33m)[m
Author: James Sewell <james.sewell@gmail.com>
Date:   Sat May 14 17:23:05 2022 +0200

    CHanged the alert and rule to be better

[33mcommit 9fb7294c4d98ca0addc47596744dbe0fe7f30d01[m
Author: James Sewell <james.sewell@gmail.com>
Date:   Fri May 13 15:26:10 2022 +0200

    Rules+Alert config w/ Alertmanager & cleanup
    
    - Recording Rules and Alert example for normal and HA
    - Alertmanager for normal and HA
    - Fixed up HA to match HA in a bunch of ways
      - scrape_configs
      - database (timescale -> postgres)
      - password
      - db args -> connstring

[33mcommit 749cf0d3d3ca3364cfe1a372059ea864042f2742[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed May 11 17:02:15 2022 -0400

    Edits to install instructions

[33mcommit f3e6cdf580512a053a7f3ff3d5e3ca5e4bff76df[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed May 11 16:10:20 2022 +0200

    Fine-tune installation documentation

[33mcommit 15dc724b856fd712596ced88b09b2d4a281008bd[m[33m ([m[1;31morigin/jg/use-docker-ha[m[33m, [m[1;32mjg/use-docker-ha[m[33m)[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu May 12 10:44:58 2022 +0200

    Post release clean up
    
    - Use `timescale/timescaledb-ha` docker image
    - Remove `startup.upgrade-prerelease-extensions`

[33mcommit f49197aac5f58755942fcbeb673271b7def122b2[m[33m ([m[1;33mtag: 0.11.0[m[33m)[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed May 11 18:40:28 2022 -0400

    Prepare for the 0.11.0 release

[33mcommit 7daa364ba8bcfaeafea8e46e49ab03ff19e81969[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed May 11 20:37:56 2022 -0400

    Fix docs formatting

[33mcommit 5b8727e2319d2cc76aa5d912f9e33b14fd84a74f[m
Author: Matvey Arye <mat@timescale.com>
Date:   Tue Mar 15 19:07:36 2022 -0400

    Switch docs to using the ha docker image
    
    This image contains a lot more tools, including toolkit.
    It is also the official image of the company.

[33mcommit c3c95d688002d459057a191630aaa534888a357e[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed May 11 16:58:24 2022 +0200

    Revert "Use ha image in Docker compose file"
    
    This reverts commit 55c64801db4c992f1b73f7cd847a4e025be9da06.

[33mcommit 55c64801db4c992f1b73f7cd847a4e025be9da06[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed May 11 16:06:35 2022 +0200

    Use ha image in Docker compose file

[33mcommit ae56c16dec2d2c38decaa4298d8b4a4ae8f5a462[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed May 11 11:41:18 2022 +0200

    Update deprecated entries in `promscale.conf`
    
    This configuration is installed when promscale is installed from the
    package repository. Some of the environment variables used in it were
    deprecated in a previous release.
    
    Closes #1348

[33mcommit 9eaf677c099b58765329ef7735ec8bfea2e835f0[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Tue May 10 12:00:41 2022 +0200

    *: format documentation and fix broken links

[33mcommit 892cf88c8d259acb530db0cdd424a172c15f2fa2[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Tue May 10 11:14:05 2022 +0200

    *: add docs formatter and checker
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit ce20e466fcb2ecea122e40cfcc1118de4b497426[m[33m ([m[1;33mtag: 0.11.0-alpha.3[m[33m)[m
Author: Matvey Arye <mat@timescale.com>
Date:   Tue May 10 20:57:22 2022 -0400

    Prepare 0.11.0-alpha.3

[33mcommit 2554e1c82f505dd7f302a4f667815185521cb74c[m
Author: Valery Meleshkin <valeriy@timescale.com>
Date:   Tue May 10 17:43:40 2022 +0200

    Moving temp table creation into the extension to be able to suppress DDL
    logging.

[33mcommit 0c32fe2b898c079bc6039c02b67675a72b0c8d4c[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sat May 7 17:24:12 2022 -0400

    Make a unique temp table for each table type
    
    Tables being copied have different structures so we name the tmp tables
    differently depending on the destination table.

[33mcommit 9a3e1ef224de5b7063e20c83e6a950f287a21d94[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Fri May 6 09:20:00 2022 -0500

    missed one

[33mcommit e99e03d5b4530e4928096b5ffff631e08529afb8[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Fri May 6 09:07:49 2022 -0500

    Reuse temp table for trace ingest
    
    Reusing the temp table will reduce catalog bloat and logging. No need to recreate it continuously.

[33mcommit 33a744307360a852ff618c79871da086a55a206d[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue May 10 14:35:56 2022 +0200

    Publish packages for ubuntu distros
    
    We were previously only publishing packages for Debian. We now cover the
    same Ubuntu versions as TimescaleDB and the Promscale Extension.
    
    Closes #1341

[33mcommit 8489d7b7fcb92da1b2772272dd853cbfb0e366bd[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Tue May 3 12:58:30 2022 +0200

    Log mandatory requirement of Promscale extension when upgrading
    
    As of version 0.11.0, Promscale extension is required for all operation.
    This commit adds logging when upgrading from older versions that the latest
    version of extension is missing and necessary to proceed.

[33mcommit df2aee4104b4216d7944cbdb112306867cbf2061[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon May 9 08:35:38 2022 +0200

    .github: add @onprem as code owner for deployment methods and monitoring mixin

[33mcommit 542db6d9df37c09fc85951a4bd4b5a3a54555c19[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Fri Apr 29 18:55:15 2022 +0000

    Update dependency otel/opentelemetry-collector to v0.50.0

[33mcommit c7acfc8f897bc2c72b39db73584ab3c16ceb7eb6[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue May 10 11:50:25 2022 +0530

    Fix tests
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 29f82da25e9ad2391a84d2562f93bf89574afc17[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue May 10 11:21:28 2022 +0530

    Replace deprecated type imports with recommended ones.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 4e42aa76f37e1a7e1d0a2f17f727d63a75ef6911[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Wed Apr 27 19:13:45 2022 +0200

    *: update data types to otel 0.49.0

[33mcommit 339c69bca8b3f1375413f8b65056bb028bb15eb7[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Wed Apr 27 18:37:59 2022 +0200

    pkg/internal/testhelpers: use new format for defining mounts
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit efdff99dc3465f198bc4123575dbe2d27cca15de[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Wed Apr 27 13:31:11 2022 +0000

    Update golang dependencies

[33mcommit 3f7aa109d5e4f81d40b8c939e17b5a6a9f5b7a4c[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue May 10 11:33:54 2022 +0530

    Update -metrics.rules.prometheus-config -> -metrics.rules.config-file
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit d3d8f940c1b1d5230bcf62e944cd75d3247a8b98[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu May 5 12:59:13 2022 +0200

    Fix output of skipped extension prerelease information

[33mcommit 4c333f781a3ba8f8fa5299753102ceeb6af59234[m[33m ([m[1;33mtag: 0.11.0-alpha.2[m[33m)[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon May 9 21:32:31 2022 -0400

    Prepare the 0.11.0-alpha.2 release

[33mcommit e7f1a6a915f4a4767fcccbc43caff82f185b75d5[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sat May 7 16:16:56 2022 -0400

    Optimize span query based on maximum time duration
    
    Using the max time duration of a trace, we can optimize the query by
    searching chunks only within the window of possibilites when we know
    the time for one of the traces of the span.
    
    We optimize the span, link, and event hypertable scans.
    
    On some tests, decreased query time from 3s to 300ms.

[33mcommit 5c4a2d1c0a169044a6577d756ee1fbabd51bf088[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sat May 7 13:50:26 2022 -0400

    Rework jaegerquery wiring to pass down config
    
    The config object will be needed to specify the max
    trace duration the query can expect.

[33mcommit e58c9f5f07cb695d48714ba73654af0afa45eb1a[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon May 9 12:14:26 2022 -0400

    Fix lint issue

[33mcommit 3638e21e81f26bdb2619717fa7f575576e002a82[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Mon May 9 13:19:57 2022 +0000

    Update github actions

[33mcommit b9eae0db06545b747035023cd9ea1603c08482d6[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Mon May 9 09:42:44 2022 +0000

    Update github actions

[33mcommit d2d893243ca0e7c528e171a960fe9d4efe6e787b[m[33m ([m[1;33mtag: 0.11.0-alpha.1[m[33m)[m
Author: Matvey Arye <mat@timescale.com>
Date:   Fri May 6 10:57:59 2022 -0400

    Prepare for the 0.11.0-alpha.1 release

[33mcommit 892f272a1fc744d0b3e16ba45d310d839a919f3b[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Apr 28 16:42:57 2022 +0530

    Add runbooks in docs.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit a287a6155b019611b0d242fccefc78d4569aba98[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri May 6 13:31:31 2022 +0200

    docker-compose: add a flag to allow promscale start and improve test resilience
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 5e25565f004424688fbb8a06e6c88c17e20abb70[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri May 6 11:10:31 2022 +0200

    deploy: regenerate

[33mcommit f5d9c8987c66621102a3e8472cd8fc64c5a47578[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu May 5 16:07:36 2022 +0200

    deploy/helm-chart: properly set dataset configuration and provide a way to set the config file for promscale
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 1c8efecc9b7a5d6fcf025dba58017e17f2194c2a[m[33m ([m[1;33mtag: 0.11.0-alpha[m[33m)[m
Author: Matvey Arye <mat@timescale.com>
Date:   Tue May 3 16:30:40 2022 -0400

    Prepare for the 0.11.0-alpha release

[33mcommit 4c64156b1d366894621849de6472e8ee0ae7401a[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed May 4 10:35:45 2022 -0400

    Fix test to use pre-release extension

[33mcommit da3d44a87dc3f6548dc522691c8a58580737273d[m
Author: Matvey Arye <mat@timescale.com>
Date:   Tue May 3 21:51:58 2022 -0400

    Fixes for new tag_map type

[33mcommit 47f6769634e686e0cacc12b57daff9ca9109cbff[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed Apr 27 17:14:11 2022 +0200

    Improve upgrade test output
    
    The general way that `diff a b` works is to show changes which were made
    to `a` to get to `b`. In PrintDbSnapshotDifferences this was the
    opposite, which made interpreting the result confusing.
    
    Additionally, the diff missed schemas which were in `b`, but not `a`.

[33mcommit 7b85613e6c40d15553cb72c8bfa02b8d0d00c5b1[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon May 2 14:00:15 2022 -0400

    Rename tracing enums
    
    This makes the enum names much more user-friendly on the SQL level.
    We made them lowercase and removed the prefix.
    
    See https://github.com/timescale/promscale_extension/pull/225 for
    more details.

[33mcommit 87dca5c2b801c5f50b1d252274bdcebc791741ec[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon May 2 11:57:02 2022 -0400

    Handle pre-release tags in CICD
    
    This PR changes things so that pre-release tag packages are
    sent to the timescaledb-exp package cloud repo and not
    the standard ones.
    
    Docker images are already correctly handled by the use of
    skip_publish = auto for the latest and major.minor tags
    in .goreleaser.yml
    
    The github release is already created in draft.

[33mcommit e319f768dec41e231aa5bc0482095c46c08819e7[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon May 2 14:09:23 2022 -0400

    Support pre-release versions of 0.5.0

[33mcommit 75c2ac4aa9f3b274e161536f8cca4cef5435d570[m
Author: niksajakovljevic <niksa.jakovljevic@gmail.com>
Date:   Tue May 3 10:06:23 2022 +0200

    Filter on jsonb hash so planner can use index
    
    Recent changes in the promscale extension introduced index on jsonb hash.

[33mcommit 0d30f11e7e5c36cab840c490a60ecb3346638cad[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon May 2 15:28:23 2022 +0200

    deploy: regenerate
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 79536c191902106423531ccde586539772c07f8b[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Apr 29 11:31:07 2022 +0200

    deploy/helm-chart: conditionally set password and user in connection secret
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 60550fadbaa11291860b1510f809fcd3e738128e[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon May 2 13:08:15 2022 +0530

    Implement `/api/v1/alerts` API.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit a973379fb364c246914476b95943ad3719d1d018[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Apr 29 18:09:30 2022 +0530

    Adds support for `/api/v1/rules` API
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit implements the `/api/v1/rules` API.
    
    This implementation is done using the rule-manager in rules package
    along with reusing the structs that Prometheus uses in its API package.
    This ensures that our API response similar to that of Prometheus.

[33mcommit 3d7cd2065d23c3612e1a2afc5a29780fd7cb0d70[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Apr 29 16:32:44 2022 +0530

    Refactor rule-manager implementation to consider noop.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit refactors the rule-manager with Manager interface that
    allows noop implementation. This allows the API to run properly even
    when the ruler is not initialized in Promscale.
    
    This commit also adds a provider in API package to support rules's access
    to the router.Rules API. The rules cannot be used in pgclient due to cyclic
    imports and poor design. Hence a provider.

[33mcommit 4bc518126503a12db056acdacb3653152b4d002c[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon May 2 09:26:55 2022 +0200

    deploy: regenerate

[33mcommit b12b97c183a993462d4958571666c6a7d3d5b65e[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon May 2 09:26:47 2022 +0200

    .github: move checking for alerts propagation to helm workflow

[33mcommit 0a87098be7103c8e7bb6b88eecdc1551140e19b4[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Tue Apr 12 09:52:55 2022 +0200

    docs/mixin: jsonnetify mixin to allow using common mixin tooling
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 498cafd512810492d1ce982ff013dd3b74c452a4[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Tue Apr 19 22:31:27 2022 -0500

    Use getter/setters for default values
    
    We refactored _prom_catalog.default and wrapped it with a getter/setter. This updates connector code that directly accessed the table to use the getter/setter instead.

[33mcommit f100e732104e2635f229ad78cbeebcba7a8b3e8f[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Apr 28 12:38:08 2022 +0530

    Simplify validation of Prometheus config and starting of rules manager.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit simplifies the Validate() func in rules package.
    
    It also removes the need for UseRulesManager variable in Config, by making it
    a function on config, which in turn solves the problem of early logging
    of alerting/ruling logs, which now no longer logs in the start of
    Promscale.

[33mcommit 42f2752a1471ea26720a8cf55e92a681b9fc4239[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Apr 27 22:00:57 2022 +0530

    Link with runner and implement manager.Stop()
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit links the rules manager with runner.go
    It also implements the manager.Stop() that stops the internal
    services of rules manager like discovery manager, notifier manager
    and the internal rules manager.

[33mcommit 400ae0c52c2966e5ae0ccfd50f57b8ec40c7e41f[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Apr 27 18:53:27 2022 +0530

    Add CLI flags and support for reading Prometheus config.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds CLI flags for rules-manager along with
    ability to read Prometheus config and validate it.

[33mcommit 21c992cfcff4ec60b254c0dae9dfb7f171e2fa36[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Apr 26 08:44:29 2022 +0200

    Minor cleanups of dataset documentation

[33mcommit f90d8d289c661993d631fe532011794f2481d735[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu Apr 14 11:15:01 2022 +0200

    .github: switch code ownership to a subteam
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 6a005c4c515594c125a240312d616b800157aa52[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Apr 25 14:45:02 2022 +0530

    Add lifecycle docs on ingest-adapter appender.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit a0cae7d8a7c3ff5a1790245e06e9080bdc55f51e[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Apr 18 15:53:56 2022 +0530

    Transfer and use local mod files for migration-tool.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit shifts the migration files from /cmd & /pkg/migration-tool
    to /pkg/migration-tool/cmd & /pkg/migration-tool/pkg/ respectively. This
    was done to avoid collision of packages between Promscale and
    Prom-migrator.
    
    Note: This need was felt when a proto panic was felt for enum
    registration since the registration was happening earlier twice -- First
    by Prom-migrator that imports prometheus/prompb and next the rules
    manager that imports the same.

[33mcommit 65a689173eb0f35d1efcb0b34d0857b74004407c[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Apr 14 17:17:20 2022 +0530

    Refactor PromQL-engine creation to be in client.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 816c9f900a13582c126ba1da05eede69e230924a[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Apr 8 18:34:58 2022 +0530

    Implement adapters for queryable and appendable.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commits adds adapters to convert our db-ingestor and samples
    querier into the appendable and queryable that is expected by the rules
    manager module.

[33mcommit bf1a706443f69007c32e94776a110c030ebc9589[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Apr 8 17:54:00 2022 +0530

    Implement managers related to ruling/alerting.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit implements discovery manager, notifier manager and rules
    manager that are required for alerting/recording rules. The managers are
    written in a way that the hot-reload in future commits can be
    implemented easily.

[33mcommit 91b4e3477212bbd0e6b5ec6c46eebeba563a1984[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Apr 6 16:36:41 2022 +0530

    Add note saying `prom-migrator` cannot migrate to HA setups.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 4edc91cb51471ab89b3e7108b4d7550661c50d0e[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon Apr 25 12:37:51 2022 +0200

    deploy/helm-chart: add ability to set dataset config parameters
    
    Resolves https://github.com/timescale/o11y-team-applications/issues/185
    Related to https://github.com/timescale/promscale/pull/1276
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit f563ae48c2686c716352e5d2f20d8d0651893c6d[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Fri Apr 22 18:07:32 2022 +0200

    Adjust to handle new default search path
    
    In [1] we set the default search path to only contain "public" schemas.
    In particular, the two schemas "_prom_ext" and "_prom_catalog" are no
    longer on the default search path.
    
    This makes usages of those functions fully schema-qualified.
    
    [1]: https://github.com/timescale/promscale_extension/pull/198

[33mcommit dfeab2fdb3c582d811db576fb5f0cde2fbe07f18[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Apr 21 11:18:37 2022 +0200

    Update logo and add link to docs page

[33mcommit e2545612885895e266d1c31fd9e7216396af4df3[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon Apr 11 15:33:32 2022 +0200

    deploy/helm-chart: remove openTelemetry.enabled option
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 477829e4c38132c1606f5f758ac0d034d36a36ef[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon Apr 18 15:18:40 2022 -0400

    Change extension schema to "public"
    
    See https://github.com/timescale/promscale_extension/pull/182

[33mcommit 6e0f9bbd89a3d3b31cad46894d277cf1cb50b3d8[m
Author: niksajakovljevic <niksa.jakovljevic@gmail.com>
Date:   Fri Apr 15 13:07:22 2022 +0200

    Remove CHECK constraints on trace tables
    
    It seems that CHECK constrains come with a huge CPU
    penalty for the database. We can instead move checks to Promscale code.
    Benchmark shows around 35% improvement in CPU usage.

[33mcommit b9a5dd824fc95f5376be301f0a5a981086ae8e00[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu Mar 24 16:18:19 2022 +0100

    deploy/helm-chart: add automatic provisioning of alerts via helm chart
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 342f61cd0efbf2c381d7448e6ad4789a8e8485fb[m
Author: niksajakovljevic <niksa.jakovljevic@gmail.com>
Date:   Wed Apr 13 16:28:33 2022 +0200

    Remove Multinode upgrade test until we fix the extension

[33mcommit f4e40d3c6605478850ddcb272b1dd76a63586c69[m
Author: niksajakovljevic <niksa.jakovljevic@gmail.com>
Date:   Sun Apr 3 19:51:37 2022 +0200

    Use trace_id custom type
    
    Some hypertable id(s) have changed due to SQL migration which drops data.

[33mcommit ea478b9eedb58fdb167a9e4803a6443402d409d3[m
Author: Valery Meleshkin <valeriy@timescale.com>
Date:   Fri Apr 8 17:55:30 2022 +0200

    Now "the oldest" image is the one on pg12.
    
    Addresses #1257.

[33mcommit eaac09e2bed14a40aa64cdfdd4b9fef5a22b1645[m
Author: Valery Meleshkin <valeriy@timescale.com>
Date:   Fri Apr 8 15:43:08 2022 +0200

    Re-enabling PG12 tests.
    
    Addresses #1274.

[33mcommit 3f5c57307a97aa54bda2711d678b194b08f99368[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed Apr 13 09:51:08 2022 +0200

    Bump minimum TimescaleDB version to 2.6.1
    
    2.6.1 delivered fixes which allow the promscale extension to be
    uninstalled.

[33mcommit ba7d8f1d0175d96b4aec67a840d0a687954f6e0c[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Mar 25 15:48:49 2022 +0100

    docs/mixin: add runbook_url link
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit f98afe3c508fad175280dac4bd77c862acb4f713[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed Apr 13 13:28:32 2022 +0200

    fix links to hotfix /rollback files

[33mcommit 9842067858af0c1801135babdcef5900025e2680[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed Apr 13 11:11:41 2022 +0200

    Add compression job performance hotfix instructions

[33mcommit 1c592ce0f68adca7b1f7dfc98848088a89a3343b[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Tue Apr 12 13:54:10 2022 +0000

    Update docker-compose

[33mcommit b02157e359ef3877ef20971b5ad23e1263807de0[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Wed Apr 13 03:27:36 2022 +0000

    Update dependency golang to v1.18.1

[33mcommit e91daaa6981d92cd730eab36ad8e078d2afee7d2[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Apr 12 13:57:22 2022 +0200

    Add test case for `reset_metric_retention_period` with two-step agg
    
    In [1] we fixed some typos in the `reset_metric_retention_period`
    function which caused the function to break when used with two-step
    continuous aggregates.
    
    This change adds tests cases to exercise `reset_metric_retention_period`
    with two-step aggregates.
    
    
    [1]: https://github.com/timescale/promscale_extension/pull/180

[33mcommit f1a36444d5040ab8ca49e2142852545f9dcb8680[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed Mar 30 01:02:08 2022 +0200

    Rename Throughput and Latency to Requests and Duration. Fix other inconsistencies. Keep variables when changing between dashboards

[33mcommit 20b6b13e4e0bab3fce2ca69e17f7e5bd1814189a[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed Mar 30 00:14:43 2022 +0200

    Use order by column index for consistency

[33mcommit 9ec2ee75ae6b17c31576026dadbd4572ae40170d[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 11:51:42 2022 +0100

    Rename duration_ms to Duration. Clean up unsused config

[33mcommit bf7a4c3bdd45d2895192cfdc02ab2da4e95002b5[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 11:32:02 2022 +0100

    Update docs/mixin/dashboards/apm-service-overview.json
    
    Co-authored-by: Ante Kresic <antekresic@users.noreply.github.com>

[33mcommit 4cd7a9122db0bfad5a8a0c084db9c157b314608d[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 11:26:10 2022 +0100

    Update docs/mixin/dashboards/apm-home.json
    
    Co-authored-by: Ante Kresic <antekresic@users.noreply.github.com>

[33mcommit f6f10c1690054902fd01f191439e526496bb54fa[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 11:26:02 2022 +0100

    Update docs/mixin/dashboards/apm-home.json
    
    Co-authored-by: Ante Kresic <antekresic@users.noreply.github.com>

[33mcommit ff3cf4ba54ce771484abac30bc1f100f06882dbb[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 11:22:09 2022 +0100

    Improve query performance of node graph queries and update queries used in variables to ensure they only those options for which there is data to be shown

[33mcommit 923881b51a962d79e3b2aef8abf47e1601866bce[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 24 09:54:40 2022 +0100

    Improve performance of common errors query by querying the span view instead of the event view

[33mcommit af672016a4d8817aa1d02d29251b4294fd401626[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Fri Mar 18 17:07:23 2022 +0100

    Fix issues related to relying on default dashboards and improve tagging

[33mcommit 2be878e966d75ddfec0e096b0493afc5581b892c[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Thu Mar 17 16:11:30 2022 +0100

    Additional improvements and adding input parameters section to select data sources for Postgres and Jaeger

[33mcommit 0ff2efa9a1b34c10dc8379d60e9f21d36c641ea9[m
Author: Ramon Guiu <ramon@timescale.com>
Date:   Wed Mar 16 10:38:58 2022 +0100

    First iteration of APM experience on Grafana

[33mcommit c8970a6cae3938c0926b36b4be1a7b87b90ea2a3[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Mar 28 14:00:15 2022 +0200

    Add Gitops dataset configuration options
    
    This change adds more options to the existing one (metrics chunk interval) for
    setting global dataset configuration using the `-startup.dataset.config` flag.
    This makes it a bit easier to configure and maintain these settings vs.
    doing it through the database directly.

[33mcommit f714452c6a5b45650c8d032457c0dd07dac4b4ef[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Mar 7 13:48:54 2022 +0100

    Remove deprecated flags for new release
    
    We have deprecated flags that need to be removed for next release. This
    change removes the flags but still attempts to detect usage of old flag
    names and old environmental variable names and logs their usage with
    recommendations to rename them to new names. Execution will stop if
    any of the old names are used.

[33mcommit ac605ef5198b668b87cdc45c41d086f487d7d7ee[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sun Apr 3 14:25:25 2022 -0400

    Better debug logs on failed tests
    
    Print the database logs when an e2e test fails.

[33mcommit 9e7afaf972886f25112841146a70d6ad818823e2[m
Author: Matvey Arye <mat@timescale.com>
Date:   Thu Mar 31 13:22:51 2022 -0400

    Add proper CICD docker image resolution in upgrade_tests.
    
    The docker image to use is resolved in the same way for
    end_to_end and upgrade tests now.
    
    Also makes upgrade tests run for the workflow dispatch,
    which is the right thing to do.

[33mcommit 7c62c3293ce3b5bde453c8cccfce475a8b15df13[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Mon Mar 21 14:12:59 2022 -0500

    Make prom_admin owner of metric tables and views
    
    Metric tables/views are created dynamically by extension-owned functions. These objects should be owned by the prom_admin.
    This makes them more secure and allows us to better control what can be done with/to these objects. They cannot be
    part of the extension since we can't make them config tables dynamically. They can't be owned by superuser since
    that would prevent dump/restore.
    
    Existing metric tables/views need to be discovered in the takeover script (0.0.0) and have their ownership altered to the superuser.
    
    * alter the search_path of the user doing the snapshots so that objects are fully qualified
    * make the docker image used a flag to enable running the tests locally
    * update docs
    * add_data_node must be called by superuser

[33mcommit d7aa13e03a8b1144ebb5f175698764a65dcc3c3e[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Mar 24 19:53:32 2022 +0530

    Fix `promscale_ingest_max_sent_timestamp` value for traces.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 8e90b2e2dfe0d8001e72349d1cee146376015509[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Sat Mar 19 14:32:41 2022 +0000

    Update golang dependencies

[33mcommit 0b7437ed3017809655d547b1564a8b2933f932a2[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Mar 22 16:16:38 2022 +0100

    Fix Docker image typo in end-to-end tests

[33mcommit 8e6a8f7c3e54410bf2fb6e263d3bb471ad7267a7[m
Author: Matvey Arye <mat@timescale.com>
Date:   Fri Mar 25 12:28:20 2022 -0400

    Fix golden file tests

[33mcommit 5b2206b8a56eb300a88435d09da4352e8a493774[m
Author: Matvey Arye <mat@timescale.com>
Date:   Fri Mar 25 10:18:36 2022 -0400

    Temporarily disable pg12 tests
    
    Will be rolled back...

[33mcommit bdcc960aa4fa9aa563b0cb306da94778dec1b27a[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon Mar 21 19:14:52 2022 -0400

    Add workflow dispatch for e2e tests
    
    Adds workflow dispatch for go-based e2e tests. Also moves
    the e2e tests into a separate workflow since the "on"
    clause is now different from other jobs.

[33mcommit 5c0067108ae41e26d5ef23cc59ee91d2b4a2b006[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Tue Mar 15 10:56:28 2022 +0100

    docs/mixin: add basic promscale dashboard
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 70161d8e3f6dcb69d0c8d0df5a3ed852811672cb[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Tue Mar 15 10:56:14 2022 +0100

    .github: add dashboard linter to CI

[33mcommit 78363830f5a419c1fda353c59c8c03941d5d8ab7[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Mon Mar 21 12:55:49 2022 +0000

    Update github actions

[33mcommit 2836fb1a01de60c6b25de80fb299347fff3d1005[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon Mar 21 12:30:16 2022 +0100

    .github: finer control over used goreleaser version

[33mcommit 0fa74860604ffcf355ce201d9593afe7da3ae498[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Fri Mar 18 14:51:11 2022 +0100

    Disable running Promscale in both HA and read-only mode
    
    This was not possible before with legacy HA, but was missed when
    updating to non-legacy HA. It doesn't make sense to try to combine these
    two operation modes.

[33mcommit 8aaf5c5ef9e38ec3c21c27e6e86a269aaf532dee[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Mon Mar 21 09:55:42 2022 +0100

    goreleaser: Fix deprecated 'use_buildx'

[33mcommit df8041acd9133ad26219d41f1e6f6dc9219a9338[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Mar 15 14:14:20 2022 +0100

    Run tests against corresponding extension docker container
    
    The main goal of this change (which is probably too large) is to ensure
    that we can build the promscale connector against a version of the
    promscale extension which contains all of the necessary bits.
    
    This change is motivated by the fact that there is now a hard dependency
    on the extension, and we need to be able to test in-progress changes
    across repositories.
    
    The basic mechanism is that for each open PR in the promscale_extension
    repo, there will be a docker image built and pushed to the registry at:
    `ghcr.io/timescale/dev_promscale_extension:<special-tag-here>`.
    
    `<special-tag-here>` is constructed from the branch name from which the
    PR originated. When a PR is opened in the promscale repository, we look
    to see if there is already a docker image corresponding to the branch
    name. If not, we fall back to the most recent docker image which was
    built on the `develop` branch in the extension repository.
    
    `<special-tag-here>` has the basic form of:
    
      `<extension-version>-<timescale-major>-<pg-version>`
    
    Some examples are:
    - `jg-ha-dockerfile-ts2-pg14`
    - `develop-ts2-pg14`
    - `0.5.0-ts2-pg14`
    
    Local development is supported through two mechanisms:
    
    1. When running e.g. `make docker-image-14` in the promscale_extension
       repo, the docker image is tagged as: `local/dev_promscale_extension`
       with the `<extension-version>` component of the tag set to `head`.
    2. When running `make test` in the promscale repo, it first looks to see
       if a local image is available matching the `local/` prefix, then
       checks for a docker image derived from the current branch name, and
       then for built from the `develop` branch.
    
    A number of things have changed tangentially as well:
    
    - end_to_end tests now take the docker image which they should run
      against as an argument instead of using some magic to derive it
    - `make test` was split into two components: `make unit` and `make e2e`
    - multinode tests are being skipped because we broke them in the develop
      branch of the promscale extension

[33mcommit 58da7b25ac581c1c7cd3bca99a8bb2ba4436c081[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Wed Feb 9 13:44:55 2022 -0600

    Make the extension required and delegate migrations to the extension
    
    This PR makes the promscale extension required, while the timescaledb extension remains optional.
    The minimum timescaledb version supported is now 2.6.0
    
    The extension now handles applying database migrations. The connector's migration logic was updated accordingly.
    
    If this is a fresh install, the connector will
    
    1. install the extension at the latest version
    
    If promscale is installed at a database version prior to 0.5.0, the connector will
    
    1. apply any pending migrations from the old way of doing migrations
    2. drop the old version of the extension if it exists
    3. install the extension at 0.0.0 which will do some "takeover" logic
    4. update the extension to 0.5.0 (or whatever the latest version of the extension is)
    
    If promscale is installed at database version 0.5.0 or greater, the connector will
    
    1. update the extension to 0.5.0 (or whatever the latest version of the extension is)
    
    Co-authored-by: Matvey Arye <mat@timescale.com>
    Co-authored-by: James Guthrie <jguthrie@timescale.com>

[33mcommit 60c6e39008d6935eecdc59fe7fd4fad94a5052cf[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Fri Mar 18 14:30:41 2022 +0100

    Remove documentation for old HA setup
    
    The old HA setup is deprecated and has been removed from Promscale.

[33mcommit bcdafad1cecc2553f78004fd8de1f4f5efe096fa[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Fri Mar 18 13:38:57 2022 +0100

    Remove legacy HA implementation
    
    There are some remnants from the legacy HA implementation which were
    still floating around in the codebase. This change removes them.

[33mcommit 8642fa5ddd6d59f639e77353bbc04b2bfbd6ddf7[m
Author: niksa <niksa@timescale.com>
Date:   Wed Mar 16 15:04:32 2022 +0100

    Fix batch IsFull check
    
    We should count the number of samples and exemplars and not the number of series.
    The count of series is usually less then the number of samples/exemplars.
    This means that batch size can have samples then our flush limit.

[33mcommit 256668e88a7156371cbfe3c8f9a95924085bb7fd[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Thu Mar 17 04:09:44 2022 +0000

    Update golang dependencies

[33mcommit 6f58451c1ffc99c59b35eaca7ab616cec2a834f9[m
Author: niksa <niksa@timescale.com>
Date:   Tue Mar 15 10:26:04 2022 +0100

    Track number of ingested bytes for metrics and traces
    
    We use the size of received Protobuf message to calculate bytes.

[33mcommit 2a7ce3b7e98b688bb3d4b41c3d4a11e673c1552e[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Mar 11 17:58:36 2022 +0530

    Refactor telemetry metric registration.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit refactors the metric registration process in telemetry. Now,
    the telemetry engine no longer requires to be passed down to each module
    that wants to send telemetry, rather just expose a func which accepts a
    telemetry engine.
    
    The call of this exposed function happens in runner.go where all modules
    that want to send telemetry are called and managed.

[33mcommit 50649992b9276a640b986f2ded61e98e7fb3623a[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Mar 11 16:01:44 2022 +0530

    Verify telemetry engine nil if TimescaleDB is not installed.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit c7fc7e9c7adc7047dbb88091933415b2ec4b7af2[m
Author: niksa <niksa@timescale.com>
Date:   Wed Mar 9 14:39:33 2022 +0100

    Expose build information through Prometheus metrics
    
    We expose version, commit and branch. This information is useful for
    comparing benchmark numbers across various builds.

[33mcommit c963c8e2416ef55c331c6dda9451417a7f584f52[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed Mar 9 10:14:04 2022 -0800

    Change end-to-end tests to work on latest db
    
    The end-to-end tests should work on the most up to date db.
    This is easier to maintain and helps find bugs in latest releases.

[33mcommit d49da565dd96fa5f642006871d0f04c725763616[m
Author: Matvey Arye <mat@timescale.com>
Date:   Mon Mar 7 15:45:03 2022 -0800

    Fixes to testing in response to PR comments
    
    Small cleanup changes.

[33mcommit 48d003d30d6768ff0dcbf760c1aa690038e8531a[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sat Feb 26 14:12:20 2022 -0800

    Require Promscale Extension in Tests
    
    Promscale extension will be required in general so make all tests
    use it.
    
    This temporarily breaks nightly timescale tests as we don't have
    nightly images that contain both the timescale and the promscale
    extension.

[33mcommit 0070f1fd5b29dc72c829bd4ee084992fe726c541[m
Author: Matvey Arye <mat@timescale.com>
Date:   Sat Feb 26 08:38:36 2022 -0800

    Refactor testing setup without TimescaleDB extension
    
    Instead of requiring a docker image without timescaleDB extension
    installed, simply remove the extension from the image if it's there.
    
    This will simplify testing scenarios where the Promscale extension
    is present but TimescaleDB is not. We won't have to create a separate
    docker image for this scenario.

[33mcommit 3a230d96b382766347ca989a5e1f8320004cf876[m
Author: Matvey Arye <mat@timescale.com>
Date:   Fri Feb 25 11:23:10 2022 -0800

    Remove testing for TimescaleDB-OSS

[33mcommit bfa90a17048d84a1568036da138be362fa5584c1[m
Author: Matvey Arye <mat@timescale.com>
Date:   Fri Feb 25 08:27:58 2022 -0800

    Deprecate testing for TimescaleDB 1.x

[33mcommit 30b2139d276c04ec2e7caf1338d1beca46a87504[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Mar 7 20:43:33 2022 +0530

    Implement gorilla mux router to unify Prometheus APIs and Jaeger APIs.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit changes the default router from prometheus/common/route to
    gorilla/mux router. This is because Jaeger's API package accepts gorilla
    router. If we didn't change the default router from the prometheus one,
    we will have to run 2 different servers which is not what we want.

[33mcommit 257dd941e498ba892d1dc3f2acce15b85cd21b83[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Mar 7 16:25:37 2022 +0530

    Update E2E tests to use internal Jaeger wrapper.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commits removes launching a new server inside trace_query
    intergration tests and instead reuses the server started in Promscale
    that directly serves the Jaeger JSON endpoints. This endpoint can be
    directly queried by Grafana/curl.

[33mcommit b12fa9447b1fea513668656ef7de351000cfdaf6[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Mar 4 20:36:42 2022 +0530

    Add Jaeger query HTTP server.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds support for running Jaeger query HTTP server
    on Promscale. This allows the users to directly query traces
    through Promscale (like via Grafana) without the need for using Jaeger-query as
    a proxy.

[33mcommit 43b0e261616d702b1478f2ef38ae0d317f4b0540[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Mar 3 16:49:50 2022 +0530

    Add support for testing database metrics module.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds E2E tests that is checked before and after running the
    database engine. If also ingests some traces to verify the evaluation of
    chunk based metrics.

[33mcommit 69109d32b4842ce0634f74d11174a81d05da9f6d[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Feb 25 18:43:55 2022 +0530

    Add alerts for database sql metrics.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds alerts for database metrics, under `promscale-database`
    group.

[33mcommit ee8a8ec1ac7367b930969b83586f1ac48e03a896[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Fri Feb 25 18:41:55 2022 +0530

    Add metrics for health-check.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds database health-check as part of database metrics. They
    are:
    1. promscale_sql_database_health_check_errors_total
    2. promscale_sql_database_health_check_total

[33mcommit 526a660b35df35af1bff00d739eda8d889c94b12[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Feb 23 16:24:40 2022 +0530

    Add support to expose database metrics in Promscale.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds a database metrics engine that takes in pgxconn and
    iterates over a list of metrics, each containing a SQL query that
    returns only one BIGINT and a corresponding prometheus metric.
    
    The engine iterates over each SQL, batches them and evaluates and then
    stores in the corresponding prometheus metric.

[33mcommit d8583e30fb6be67625985bf9821ea46a590318a7[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Mar 3 08:58:14 2022 +0100

    Add missing word to slow ingestion warning

[33mcommit 36e89079632e1e76f1d5a83f9c2be5a465277db4[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Mar 3 17:46:24 2022 +0530

    Add ability to stop routines on partial error.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit d3fc7be92ca9b85e5cbb4e2153f3c2630c837fc1[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Mar 3 13:25:59 2022 +0530

    Improve logging of active servers on start.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit a7e71e7c3f6f03443a4ddacb022357a4e8797b08[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Wed Mar 2 11:29:23 2022 +0100

    Enable tracing flags by default
    
    We are moving tracing support out of optional to on by default. This means
    removing the --enable-feature option for tracing and logging a warning
    if somebody is still using it unnecessarily. OTLP GRPC server will
    always be started by default on port 9202.

[33mcommit 9a70a6a1d559d7a1e9ea043fe20c3def4ef0ced3[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Thu Mar 3 01:17:45 2022 +0000

    Update dependency otel/opentelemetry-collector to v0.46.0

[33mcommit a5fc5fd2fcbdd3e247691a32257db2a7a90f42e9[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Mar 3 10:34:43 2022 +0100

    renovate: Do not show version of dependency being update for gomod
    
    The gomod manager never puts the name of the module being updated in the
    title, so putting the version in the title is just confusing.

[33mcommit 14a7774c9734eb4258eec804c04c59910947d229[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Thu Mar 3 01:19:30 2022 +0000

    Update golang to v0.46.0

[33mcommit 1cd4dcc9c9e3ed082f3e2521ffe050f4c4f7e0ae[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Wed Mar 2 07:24:01 2022 +0000

    Update github actions

[33mcommit dd6fea0810f25e394129c1d8da875086480e19ac[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Tue Mar 1 20:50:53 2022 +0000

    Update github actions

[33mcommit 8d88c5b9411289900d14b6f8bd3ba1891f27f3c6[m
Author: niksa <niksa@timescale.com>
Date:   Fri Feb 25 13:02:55 2022 +0100

    Ingest traces using COPY FROM
    
    Using Postgres COPY FROM resulted in 2X improvements for trace ingest performance.
    Here we use COPY FROM for inserting spans, links and events into respective hypertables.
    COPY FROM uses binary protocol so it's expected to perform well.

[33mcommit e37886ede4891213a31a44414090f1604e7a6b5e[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Mar 1 09:47:56 2022 +0100

    Add JamesGuthrie as owner of pgmodel/querier

[33mcommit 9e79cb3241a863d7c2d31e0537143aafc1a858c6[m
Author: niksa <niksa@timescale.com>
Date:   Mon Feb 28 14:35:52 2022 +0100

    Codeowners update

[33mcommit 231904acec46cc515a5f867d5daf5bb7e6407a4a[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Feb 23 19:20:32 2022 +0530

    Add alerts for database check metrics.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit e7eae3149e92f104d2f096375762cbae4a5aef3a[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue Feb 22 17:13:34 2022 +0530

    Add database metrics into pgxconn package.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit updates the implementation of pgx in pkg/pgxconn
    package and implements the following database metrics:
    1. promscale_database_requests_total
    2. promscale_database_request_errors_total
    3. promscale_database_request_duration_seconds

[33mcommit 71acec0ef908f2928fadc8c2280269f066cd5350[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Feb 28 12:16:37 2022 +0100

    Fix testcontainers breaking change
    
    https://github.com/testcontainers/testcontainers-go/pull/354

[33mcommit c49026f9e55b799d12befff49b9eed2e45609f7f[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Mon Feb 28 08:52:39 2022 +0000

    Update golang

[33mcommit d7abe5dd84b9001be9309517fe8e366ba0541fa6[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Mon Feb 28 11:27:22 2022 +0100

    .github: control tooling version used in CI via renovate
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 3f31aca1dc391a772dd94c15c04d12d184716118[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Feb 28 09:23:21 2022 +0100

    Make Renovate bot ignore pinned Prometheus version
    
    Prometheus library versions need to be pinned by us in order to work
    properly with Promscale.

[33mcommit 38fdad02aaef3a7f5225673dbd65cb42179c5f84[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Sun Feb 27 00:46:28 2022 +0000

    Update docker-compose

[33mcommit 4848c4c252d23f77cb114065c938f48eea3f0449[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Sat Feb 26 00:30:52 2022 +0000

    Update dependency golang to v1.17.7

[33mcommit cb07f26f00e57ab47bc11603822698e9e84163eb[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Feb 25 12:12:25 2022 +0100

    *: add testing alert syntax in CI
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 892f57c67bd7edff51698b7240f7c0e91cefcba3[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Feb 25 12:03:41 2022 +0100

    .github: fix setting labels on PRs by renovate bot

[33mcommit 89e91d06619a4c7511c7ae927dcee6533fc385f9[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Feb 25 11:57:17 2022 +0100

    .github: move renovate config to decluter TLD

[33mcommit e617740b2836b1fab3337262303c15e2047a6314[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Fri Feb 25 11:01:45 2022 +0000

    Update golangci/golangci-lint-action action to v3

[33mcommit 2d9f392c80f43084a5ef4fff00b1f15b6dd6b925[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Fri Feb 25 11:37:50 2022 +0100

    .github: remove dependabot

[33mcommit 5df37a7f422c1abf278911914284b5f133b088c2[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu Feb 24 17:17:42 2022 +0100

    *: reconfigure renovate-bot
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit a539a2002ab549d35d319e6f4d7e87dc6476a67b[m
Author: Renovate Bot <bot@renovateapp.com>
Date:   Thu Feb 24 16:15:43 2022 +0000

    Add renovate.json

[33mcommit 02e6a1b1a15f0d82f9799ebc647bc5c83f46d0dd[m
Author: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>
Date:   Wed Feb 23 17:19:57 2022 +0000

    Bump securego/gosec from 2.9.6 to 2.10.0
    
    Bumps [securego/gosec](https://github.com/securego/gosec) from 2.9.6 to 2.10.0.
    - [Release notes](https://github.com/securego/gosec/releases)
    - [Changelog](https://github.com/securego/gosec/blob/master/.goreleaser.yml)
    - [Commits](https://github.com/securego/gosec/compare/v2.9.6...v2.10.0)
    
    ---
    updated-dependencies:
    - dependency-name: securego/gosec
      dependency-type: direct:production
      update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] <support@github.com>

[33mcommit e6ef77b403d86575adbb096f911b41d6e9fa9491[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Feb 8 10:45:49 2022 +0100

    Remove StartTime and EndTime from QueryHints
    
    The StartTime and EndTime values stored in QueryHints were superfluous,
    and can be derived from other time ranges which are already available.

[33mcommit 7b233db8a82beb8e202c964e18aa95607efca656[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Mon Feb 14 13:18:28 2022 +0100

    Fix documentation around `add_prom_node`
    
    `add_prom_node` is a procedure, so it must be invoked with `CALL`.
    
    Closes #1010

[33mcommit e49e74b0f9688b57b8444e198cc8859da02a30d3[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Feb 8 11:10:29 2022 +0100

    Provide example for configuration via config.yml

[33mcommit 01fc4c16af1f10a12538ea4c48de0ccea3912332[m
Author: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>
Date:   Mon Feb 21 10:48:09 2022 +0000

    Bump github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger
    
    Bumps [github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger](https://github.com/open-telemetry/opentelemetry-collector-contrib) from 0.44.0 to 0.45.1.
    - [Release notes](https://github.com/open-telemetry/opentelemetry-collector-contrib/releases)
    - [Changelog](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/open-telemetry/opentelemetry-collector-contrib/compare/v0.44.0...v0.45.1)
    
    ---
    updated-dependencies:
    - dependency-name: github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/jaeger
      dependency-type: direct:production
      update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] <support@github.com>

[33mcommit 2700b27f042bd6d4cbda4a08c33ef284dcefd1b3[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue Feb 22 16:12:39 2022 +0530

    Add alerting rules for Promscale.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit b652d76712129eff1581566ad6b0dcdc26f0751b[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Mon Feb 14 13:06:04 2022 +0100

    Fix broken link in README
    
    The link pointed to the old tutorial, Promscale has its own section now.
    
    Closes #1086

[33mcommit 65fbad13195ee83eb44c570106d900bfd908c288[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Tue Feb 22 11:31:00 2022 +0100

    Log warning when remote read request is cancelled midway
    
    Previously, we were trying to update the header but that cannot be
    done in this instance since we are already starting to write the
    request body. Only valid way of handling these situations is to
    log the error and finish the request.

[33mcommit 4247167781ea3900450591753f9bac3067de5f80[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Feb 17 20:09:53 2022 +0530

    Fix ingested_samples telemetry by using other name for received insertables.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 301d9f65e65d9c0b81e4f14ee1ed76fadaf1c12c[m
Author: Paweł Krupa <pawel@krupa.net.pl>
Date:   Wed Feb 16 14:36:29 2022 +0100

    Delete .travis.yml

[33mcommit 609311e3116fda484b8908bfb65d0f8aabf16460[m
Author: niksa <niksa@timescale.com>
Date:   Mon Feb 21 14:17:21 2022 +0100

    Upgrade PGX to latest master
    
    There has been a bug in a way PGX reacted to context timeouts
    https://github.com/jackc/pgx/issues/1156 .
    We probably want to upgrade to next PGX release once it's out.

[33mcommit 8e871f050a73f9533094d53fc44b6a7bb1a69db5[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Feb 21 15:57:18 2022 +0530

    Register `promscale_ingest_channel_len` metric and make it a Gauge.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 5b2c63c30d5a0e084706f2e988e84d96fa124c0a[m
Author: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>
Date:   Fri Feb 18 12:06:34 2022 +0000

    Bump go.opentelemetry.io/collector/model from 0.44.0 to 0.45.0
    
    Bumps [go.opentelemetry.io/collector/model](https://github.com/open-telemetry/opentelemetry-collector) from 0.44.0 to 0.45.0.
    - [Release notes](https://github.com/open-telemetry/opentelemetry-collector/releases)
    - [Changelog](https://github.com/open-telemetry/opentelemetry-collector/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/open-telemetry/opentelemetry-collector/compare/v0.44.0...v0.45.0)
    
    ---
    updated-dependencies:
    - dependency-name: go.opentelemetry.io/collector/model
      dependency-type: direct:production
      update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] <support@github.com>

[33mcommit 6528ab1de330a4f2086c64dfa7f403aead125598[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Feb 21 11:12:51 2022 +0100

    Update E2E test timeout to 30 min
    
    Recently multinode has been timing out for most PRs. Increasing the duration
    to 30 mins until we can see how long it takes now in reality. We can reduce
    further down the line.

[33mcommit 72d4962fef85fd4749619aadb4ce66fdcc2801f3[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Fri Feb 18 10:47:37 2022 +0100

    Reduce dependabot to update Go deps weekly
    
    Some of the dependencies release new versions very quickly leading to
    a lot of PRs generated by dependabot. Reducing the frequency to weekly
    makes it more manageable to maintain the dependencies.

[33mcommit 5484848ec24df46c5ffccb54646422d97fb0814b[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu Feb 17 11:12:50 2022 +0100

    move goreleaser config to cannonical location

[33mcommit 53a189888ae0eff178321a3d8613324a4a97fd7c[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Fri Feb 18 09:02:39 2022 +0100

    Bump otel deps

[33mcommit d2f39eef5bc63adf59bcf1f2133473bec3bcd75b[m
Author: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>
Date:   Thu Feb 17 17:28:34 2022 +0000

    Bump go.opentelemetry.io/otel/exporters/jaeger from 1.3.0 to 1.4.1
    
    Bumps [go.opentelemetry.io/otel/exporters/jaeger](https://github.com/open-telemetry/opentelemetry-go) from 1.3.0 to 1.4.1.
    - [Release notes](https://github.com/open-telemetry/opentelemetry-go/releases)
    - [Changelog](https://github.com/open-telemetry/opentelemetry-go/blob/main/CHANGELOG.md)
    - [Commits](https://github.com/open-telemetry/opentelemetry-go/compare/v1.3.0...v1.4.1)
    
    ---
    updated-dependencies:
    - dependency-name: go.opentelemetry.io/otel/exporters/jaeger
      dependency-type: direct:production
      update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] <support@github.com>

[33mcommit da8c0a9fce497626e3d44b798f58341137745905[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Feb 17 15:45:48 2022 +0530

    Prepare for the next development cycle

[33mcommit 0742e64f1c1371f6d83fa761f45b54dc4e69e5a8[m[33m ([m[1;33mtag: 0.10.0[m[33m, [m[1;31morigin/release-0.10[m[33m)[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Feb 17 14:29:34 2022 +0530

    Prepare for the 0.10.0 release.

[33mcommit 1ec7c26dc7c4d53688e8054a46287af592a3c758[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Thu Feb 17 13:37:20 2022 +0530

    Fix changelog: Add example tracing setup to unreleased.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit cf44600a5d655ea600b008bfd9fa6ee6ff9985c0[m
Author: niksa <niksa@timescale.com>
Date:   Tue Feb 15 22:14:39 2022 +0100

    Increase visibility into trace ingest
    
    Add few more metrics that will give us more insight into ingest.
    Mainly we want to see how big our insert batches are and also the time it
    takes to insert various trace related objects. This should help
    us in finding weak spots.

[33mcommit 67c03fa611e41d3decd62930a32be2419c79b5e5[m
Author: Matvey Arye <mat@timescale.com>
Date:   Wed Feb 16 15:51:47 2022 +0200

    Upload release to packagecloud

[33mcommit ab16e62958abce1fc3108cd3cba6ecffdc1c8dc6[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Wed Feb 16 16:05:45 2022 +0100

    deploy/helm: sanitize extra environment variables

[33mcommit 73d7c716e0059d2ef62f30b83cde30dc48c1bcf1[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Tue Feb 8 10:23:37 2022 +0100

    Add support for OTEL collector endpoint in Promscale tracing telemetry exporter
    
    In addition to Jaeger, now Promscale can send its telemetry data to OTEL
    collector endpoint. Since this is an endpoint that Promscale itself exposes,
    we can actually ingest tracing data from Promscale into Promscale itself.

[33mcommit 9391f8cacb29b1268a6826c46f79a2a77e6d4dd0[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Feb 16 18:58:20 2022 +0530

    Fix alter column to be in 0.10.0-dev and not in 0.8.0-dev
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit fixes the wrongly added
    2-alter_promscale_instance_information_column.sql to 0.8.0 when the next
    release scheduled was 0.10, meaning the changes to schema wont be
    applied. This was a bug.
    
    This commit shifts the alter column file to where it should be, i.e.,
    0.10.0/1-alter_promscale_instance_information_column.sql
    which means, the existing users when update to 0.10 will have the alter
    column.

[33mcommit d08c13811123ffb4ec850750cb7720cc6e03a80b[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Feb 16 15:29:55 2022 +0530

    Implement ingested_spans_count in telemetry data.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>

[33mcommit 4ed7edc16e0a635b997af6e36fd0dc4ce83a91e6[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Mon Feb 14 10:10:50 2022 +0100

    Add logging usage of deprecated flags with recommendations
    
    In version 0.11.0 we will be removing a lot of old CLI flags. Users will
    have to update their configuration to new flag names when that happens.
    This is the first step in handling this transition.

[33mcommit 2ba6586b2406d181e3998ba2035119fc00a8f1d9[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Thu Feb 10 14:03:56 2022 +0100

    Update removal of deprecated flags to version 0.11.0

[33mcommit f59b23e3d8ed4b4da91b310ead0097131e71a5de[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Thu Feb 10 13:55:26 2022 +0100

    Add panic on missing flags when applying flag aliases
    
    This is used to verify that old flag aliases are attached to new flag
    names without missing any flags.

[33mcommit a84f430cd9cd3bfdcb1df1a8cd5d512070a83f09[m
Author: Ante Kresic <ante.kresic@gmail.com>
Date:   Thu Feb 10 13:54:28 2022 +0100

    Fix web flags which were missed during the flag rename

[33mcommit e7eda923ecb25f1d47aac676790f6c38b9a58b7d[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Wed Feb 16 10:59:50 2022 +0530

    Add doc to show new metrics exposed by /metrics endpoint.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit adds a markdown file that shows the newly added/refactored
    metrics exposed by /metrics endpoint.

[33mcommit 6a73d8c13c70d5034e1a19f96bcd449f7c67937d[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue Feb 8 14:39:43 2022 +0530

    Refactor metrics for api package.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit abstracts the metrics in API package with
    a statsUpdater interface along with unifying them invalidReq
    metric into the requests metric, represented by the label {code="400"}.

[33mcommit f83a39d94011021f2a299f892be1def7f5fbc151[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Tue Feb 8 13:27:41 2022 +0530

    Make telemetry engine to accept multiple metrics.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    Until now, the telemetryEngine's RegisterMetric() accepted only 1 metric
    that required to be monitored. However, with metric renaming work,
    metrics like
    promscale_executed_queries required 2 series to be considered, like
    promscale_query_requests_total{handler="/api/v1/query", code="2xx"} and
    promscale_query_requests_total{handler="/api/v1/query_range",
    code="2xx"}. These are two series but in metric convention, work as different metrics
    (prometheus_metric.With(label-sets) gives
    a metric).

[33mcommit 7101ab1cd551fa2a7b8c93fb0724ee98f65e422f[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Feb 7 18:42:02 2022 +0530

    Rename metrics in querier.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit renames metrics in querier component of promscale. This is
    how the metrics look now:
    
    1. promscale_query_requests_total{code="2xx", handler="/api/v1/query",
    instance="localhost:9201", job="promscale", type="metric"}
    2. promscale_query_duration_seconds_bucket{handler="/api/v1/query",
    instance="localhost:9201", job="promscale", le="0.1", type="metric"}
    3. promscale_query_remote_read{handler="/read",
    instance="localhost:9201", job="promscale", type="metric"}

[33mcommit 75daab31d4edfa6f1bf42e827e4e7f0eab0e6710[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Feb 7 16:21:41 2022 +0530

    Rename and add perf metrics to clockcache.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit renames cache metrics as per the new design and adds
    performance metrics to clockcache. Following metrics are
    implemented/updated:
    
    promscale_cache_enabled{type="label", name=”cache-name”}
    promscale_cache_capacity_bytes{type="label", name=”cache-name”}
    promscale_cache_capacity_elements{type="label", name=”cache-name”}
    promscale_cache_elements{type="label", name=”cache-name”}
    promscale_cache_evictions_total{type="label", name=”cache-name”}
    promscale_cache_queries_hits_total{type="label", name=”cache-name”}
    promscale_cache_queries_total{type="label", name=”cache-name”}
    promscale_cache_queries_latency_bucket{type="label", name=”cache-name”}
    promscale_cache_queries_latency_sum{type="label", name=”cache-name”}
    promscale_cache_queries_latency_count{type="label", name=”cache-name”}
    
    However, statement_cache has promscale_cache_enabled,
    promscale_statement_cache_per_connection_capacity and
    promscale_cache_elements_histogram since it is not a clockcache.

[33mcommit ead44af7f961f871ca9034abf453bc1a4a556b69[m
Author: Harkishen-Singh <harkishensingh@hotmail.com>
Date:   Mon Feb 7 13:24:37 2022 +0530

    Rename metrics for ingestor module + some refactorings.
    
    Signed-off-by: Harkishen-Singh <harkishensingh@hotmail.com>
    
    This commit basically refactors metrics in ingestor module to
    pkg/metrics/ingestor.go
    
    It follows the convention of basically 3 labels:
    1. type: used to differentiate between 2 category of metrics in
    promscale which are either 'trace' or 'metric'.
    2. subsystem: this refers to an internal system of the module, like
    the 'copier' or 'metric_batcher'.
    3. kind: this categorizes the metrics that are common between 'samples',
    'metadata' and 'exemplar' or 'spans'.

[33mcommit ab8154bed6c6cfc31ae936f3d37e468751f01142[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Mon Feb 14 07:59:45 2022 +0100

    Use fully-qualified schema for _prom_catalog.ha_leases
    
    Without a fully-qualified schema, these SQL scripts fail to run when
    executed during the promscale extension install process.

[33mcommit 941cfd29ea2f110446729bd7ea9dcf2b3f151b3f[m
Author: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>
Date:   Thu Feb 10 12:50:29 2022 +0100

    *: Do not publish release automatically

[33mcommit 1ceecae7d72044839eb3f9538727274cd39b410b[m
Author: Paweł Krupa <pawel@krupa.net.pl>
Date:   Tue Feb 8 16:44:02 2022 +0100

    deploy/helm: fix order of templating
    
    To use `tpl` function, content needs to be first unpacked/serialized with `toYaml` and not the other way round.
    
    Signed-off-by: Paweł Krupa (paulfantom) <pawel@krupa.net.pl>

[33mcommit 856e47781637fdd70a7c908b4db72350dbf53c3b[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Feb 3 12:52:25 2022 +0100

    Refactor out RemoteReadQuerier interface
    
    The Querier interface was a weird mixture of "method to query" and
    "methods to get things with which to query". This change moves "method
    to query" into its own interface.

[33mcommit b1c5c08407a08b979e2c21aa766a7f570c5cff01[m
Author: niksa <niksa@timescale.com>
Date:   Fri Feb 11 11:28:43 2022 +0100

    Don't log success message if extension update failed

[33mcommit 32696d6f9e8651fa2d6c100d87fc03b759457bf1[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Feb 10 14:26:10 2022 +0100

    Skip failing multinode trace test

[33mcommit ebbde6ffea6d02d17d9d6d2e65ad8d16cc890391[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Feb 10 12:58:57 2022 +0100

    Skip failing telemetry tests related to different timescaledb versions

[33mcommit 0d66b9b65688cf80f3c7578a4ab7611ac17ce1ff[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Thu Dec 9 19:25:03 2021 +0100

    Prevent swallowed build errors in e2e tests
    
    GitHub's documentation on `run` [1] and `shell` [2] implies that
    commands invoked via bash will be run with `pipefail`. Unfortunately
    this is not the case. Specifying `bash` as the `shell` does activate
    `pipefail`, and prevent errors in test execution from being swallowed.
    
    [1]: https://docs.github.com/en/actions/learn-github-actions/workflow-syntax-for-github-actions#jobsjob_idstepsrun
    [2]: https://docs.github.com/en/actions/learn-github-actions/workflow-syntax-for-github-actions#jobsjob_idstepsshell

[33mcommit 74f66b030b9d9d8a6f42aa978f04f8485a22e1d7[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Tue Feb 1 18:27:34 2022 +0100

    Fix broken pushdown queries
    
    Under certain circumstances, pushdown queries using promscale-extension
    aggregates fail.
    
    The failure comes from the fact that the start and end parameters passed
    to the aggregate don't correctly align with the actual data which is
    aggregated over by the query.
    
    A call to the prom_rate aggregate looks something like the following:
    
    SELECT
        prom_rate(start, end, step_size, range, sample_time, sample_value)
    FROM a_metric
    WHERE a_metric.t >= data_start
      AND a_metric.t <= data_end
    
    If the time span [data_start, data_end] delivers results which are
    outside of the time span [start, end], the aggregate function errors.
    
    In general, this bug appeared when an aggregate pushdown function (such
    as PromQL rate) is used together with a vector selector, such that the
    lookback of the vector selector is larger than that of the aggregate
    pushdown.
    
    The fix is to build the SQL query using the correct values for
    data_start and data_end.

[33mcommit 9e90a385080120e122ffc02901dcdcbe910bedb5[m
Author: James Guthrie <jguthrie@timescale.com>
Date:   Wed Feb 9 16:44:01 2022 +0100

    Do not attempt to push down calls with offsets
    
    We don't correctly handle pushdowns with offsets, so we just skip them
    instead of returning erroneous results.

[33mcommit 556c7bd65419d81d10946fa652751a139e51e1bf[m
Author: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>
Date:   Wed Feb 9 17:22:45 2022 +0000

    Bump actions/setup-go from 2.1.5 to 2.2.0
    
    Bumps [actions/setup-go](https://github.com/actions/setup-go) from 2.1.5 to 2.2.0.
    - [Release notes](https://github.com/actions/setup-go/releases)
    - [Commits](https://github.com/actions/setup-go/compare/v2.1.5...v2.2.0)
    
    ---
    updated-dependencies:
    - dependency-name: actions/setup-go
      dependency-type: direct:production
      update-type: version-update:semver-minor
    ...
    
    Signed-off-by: dependabot[bot] <support@github.com>

[33mcommit 32c9aa101b516aa30172d2f632f4968f9b1e31d5[m
Author: John Pruitt <jgpruitt@gmail.com>
Date:   Mon Jan 31 11:08:04 2022 -0600

    Ensure that span start <= end
    
    Adds code to detect when a span's end time is less than its start time. This would be caught by a database constraint and prevent the insertion of the span. The new code will swap the start and end times to allow the span's insertion.
