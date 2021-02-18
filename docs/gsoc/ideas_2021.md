# Google Summer of Code 2021

This document contains ideas that are participating in GSoC 2021.

### Project Ideas table

| **Project** | **Number of related ideas** |
|:--------------------------|:-----:|
| **Promscale**                 | 1 |
| **Prom-migrator**             | 2 |
| **Tobs**                      | 1 |
| **Remote-write-benchmarker**  | 1 |

**Note:**
* We recommend that you contribute to the projects in code, before writing the proposal.
* If you plan to include something else in the proposal than the mentioned ideas, you are most welcome to do so. But to 
  confirm if it’s in the interest of the maintainers to mentor it, please have a conversation with them before
  proceeding. This will help save your precious time.

## Ideas

### Enable Prom-migrator to act as a live-streaming server and integrate it with Promscale

**Related-projects:** Promscale, Prom-migrator

**Skills:** Golang

**Explanation**

Promscale is a long term, reliable storage system for Prometheus data, that is horizontally scalable and can handle
petabytes of data. It is built on top of TomescaleDB. It enables Prometheus data to be queried via PromQL and SQL.

Prom-migrator is an open-source officially listed, Prometheus data migration project, that is capable of migrating
several terabytes of data while being light on resources. It is matured enough to be resistant to interruptions and
resume from last migrated data, irrespective of how large the migration is. For more information visit [Prom-migrator's
blog post](https://blog.timescale.com/blog/promscale-analytical-platform-long-term-store-for-prometheus-combined-sql-promql-postgresql/).

Currently, Prom-migrator is a binary tool that performs data migration. The idea is to implement changes in the tool,
such that it can be used:
1. As a server so that it can run as a stateless application and perform migration in regular intervals.
2. Exposed as a function so that other remote-storage systems that support Prometheus remote-write can import the
   function and get the live-streaming functionality.

The exported function should also be integrated with Promscale. This will enable live-streaming of samples from a storage
system, in regular intervals which can be enabled or disabled via a CLI flag.

**Mentor(s):** Harkishen

### Automated E2E testing of Prom-migrator with other remote-storage systems via a single command

**Related-projects:** Prom-migrator

**Skills:** Golang, writing tests in Golang, some familiarity with Docker

**Explanation:** Prom-migrator is a Prometheus data migration tool that migrates data from one remote-storage system to
another. Currently, we do various integration tests ([see here for reference](https://github.com/timescale/promscale/tree/master/pkg/migration-tool/integration_tests))
to make sure that the migrator is behaving
as per what is the intent.

The aim of this idea would be to have end-to-end tests, written in Golang. These tests will insert data into one system
and then carry out actual migration from one system to another in docker containers.

As of now, Prometheus has 27 remote-storage systems listed officially ([link](https://prometheus.io/docs/operating/integrations/)).
It would be great to cover as many
remote-storage systems as possible in the implementation process.

The advantage of this idea would be that it will help find any incompatibilities of some storage system in the future. This
will let us know at the earliest about any such happenings and then updating Prom-migrator to meet the new requirements.

**Note:** Though there are 27 remote-storages, the approach would be the same for all. So, an implementation for two
remote-storage systems, to carry out migration between should be reusable with the rest of the storage systems, by
simply replacing the docker image of it.

**Mentor(s):** Harkishen

### Use Helm as a library in Tobs

**Related-projects:** Tobs

**Skills:** Golang, Helm

**Explanation:**

Tobs is a CLI tool designed to install all the observability stack you need for monitoring in your Kubernetes cluster
and provides complete life cycle support on your monitoring stack. It abstracts all the actions for the observability
stack with a single cmd. Currently, tobs uses helm CLI internally for deploying stack and upgrading it. Using helm as a
CLI has a prerequisite to install helm before using tobs. Also, any changes in helm CLI will break tobs.

At present tobs only uses helm CLI functionalities like install, show, uninstall, repo. We need to get rid of helm
dependency as CLI and leverage helm by plugging it as a library into tobs. This will allow us to take advantage of helm
offerings as a library and to add more features into tobs. As translating this helm dependency from CLI to library
shouldn't take a long time. We expect e2e tests on the work done and in improving the existing tobs tests suite.

**Mentor(s):** Vineeth

### Automate running the remote-write-benchmarker

**Related-projects:** Remote-write-benchmarker

**Skills:** Golang, React

**Explanation:**

Remote-write-benchmarker is a tool that leverages Prometheus’s remote-write component. It is used to benchmark the
maximum performance of different storage systems that can be achieved in conjunction with Prometheus’s remote-write
component.

Remote-write-benchmarker can also be used to understand how Prometheus’s remote-write component behaves with the
performance of the remote-storage systems. This helps various Prometheus remote-storage vendors to tune their
performance in a way that they can maximize the performance.It can also be used to compare the write performance of
various systems.

But right now the process of setting up and running all the components is manual. This project will automate it:
1. Automate bringing up various components and starting the benchmarker.
2. provide real-time updates of the current benchmarking state, during the course of the benchmarking process.
3. Monitor and record resource usage of the components during the benchmark.
4. Record benchmarking results.
5. Ability to run a “matrix” of various configuration options on the benchmarker and components.
6. Show the end result is a web UI.

This tool will handle everything end-to-end. Once it is started, it will spin up a Kubernetes cluster, set up the
required systems, initiate the test, provide real-time updates, stop or pause the test, and shut down the cluster
(on user call) and show the performance outputs.

**Mentor(s):** Matvey, Harkishen

