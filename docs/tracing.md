# Tracing

**Note:** tracing support in Promscale was added in version 0.7.0 and is currently in beta. This means we can’t ensure backwards compatibility with the data model in future versions. It also means that you may experience stability and performance issues. Please help us improve Promscale by [reporting issues](https://github.com/timescale/promscale/issues/new).

## Overview

Promscale has native support for [OpenTelemetry](https://opentelemetry.io/) traces via the OpenTelemetry protocol (OTLP). Promscale supports the entire OpenTelemetry trace data model including spans, events and links.

Promscale also supports Jaeger and Zipkin traces via the OpenTelemetry Collector. The OpenTelemetry Collector ingests data from Jaeger and Zipkin instrumentation, converts it to OpenTelemetry and sends it to Promscale using OTLP.

You can query the traces in Promscale using the full TimescaleDB and Postgres' SQL capabilities. This allows you to get very deep insights from your tracing data to understand problems and identify optimizations for your applications.

Promscale integrates with the Jaeger UI and Grafana to explore and visualize you traces.

## Installation

### Kubernetes

The instructions in this section use [tobs](https://github.com/timescale/tobs). Tobs makes it easy to install a complete pre-configured observability stack for traces (and metrics!) on Kubernetes. If you don’t want to use tobs, check the instructions to deploy Promscale on [Docker](#docker).

First, install tobs:

```bash
curl --proto '=https' --tlsv1.2 -sSLf  https://tsdb.co/install-tobs-sh |sh
```

Once installed make sure tobs, the executable, is in your path. When you install it you’ll see some output asking you to copy the file to your system bin folder or to add it to your path. This is important because if you installed tobs in the past you may run the older version without noticing which doesn’t include tracing support. To check which version of tobs you’re running use the following command:

```bash
tobs version
```

You will see an output similar to

```
Tobs CLI Version: 0.7.0, latest tobs helm chart version: 0.7.0
```

Make sure both the CLI and the helm chart version are 0.7.0 or higher.

Then use tobs to deploy a full observability stack with a single command to your Kubernetes cluster. Note that tobs requires `kubectl` installed and configured to connect to the cluster where you want to deploy Promscale.

```bash
tobs install --tracing
```

Notice the `--tracing` parameter. This will install the latest beta version of Promscale with tracing support. If you omit this parameter tobs will install the latest stable version of Promscale which does not include tracing support.

And that’s it!

Next step is to configure your services to [send traces to Promscale](#ingest-traces-into-promscale).

### Docker

Promscale consists of the Promscale Connector and TimescaleDB.

There are images on DockerHub for both the [Promscale Connector](https://hub.docker.com/r/timescale/promscale/tags) and [TimescaleDB with the Promscale extension](https://hub.docker.com/r/timescaledev/promscale-extension).

For the Promscale Connector we recommend using the latest version with tracing support (`0.7.0-beta.latest`). For TimescaleDB with the Promscale extension the latest version with more recent TimescaleDB and Postgres versions (`latest-ts2-pg13`).

There are many different ways to deploy Docker images that we will not cover here. Below we provide an example of how to run Promscale using Docker on a host.

First, let's create a network specific to Promscale:

```bash
docker network create --driver bridge promscale-timescaledb
```
Then let's run TimescaleDB with the Promscale extension (replace `<password>` with your selected password):

```bash
docker run --name timescaledb -e POSTGRES_PASSWORD=<password> -d -p 5432:5432 --network promscale-timescaledb timescaledev/promscale-extension:latest-ts2-pg13 postgres -csynchronous_commit=off
```

Finally let’s run the Promscale Connector with tracing enabled (replace `<password>` with the password you selected in the previous step):

```bash
docker run --name promscale -d -p 9201:9201 -p 9202:9202 --network promscale-timescaledb timescale/promscale:0.7.0-beta.latest -db-password=<password> -db-port=5432 -db-name=postgres -db-host=timescaledb -db-ssl-mode=allow -otlp-grpc-server-listen-address=:9202
```

**Note**: `db-ssl-mode=allow` is just for explanatory purposes. In production environments, we advise you to use `db-ssl-mode=require` for security purposes.

## Ingest Traces into Promscale

There are three main trace data formats: OpenTelemetry, Jaeger and Zipkin.

Promscale has native support for ingesting OpenTelemetry traces which means you can send OpenTelemetry traces directly to Promscale using the correponding OTLP exporters. You can send OpenTelemetry traces directly or via the [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector). For anything beyond a simple evaluation using the OpenTelemetry Collector is recommended. It centralizes management of all your telemetry data so you can easily define processing rules and where you want to send the data.

Jaeger and Zipkin trace formats are supported via the OpenTelemetry Collector only.

Let’s look first at how to set up the OpenTelemetry Collector and then at how to configure OpenTelemetry, Jaeger and Zipkin instrumentation to send traces to Promscale.

### OpenTelemetry Collector

If you used tobs to deploy Promscale, then the OpenTelemetry Collector has already been deployed into your cluster and you can move to the OpenTelemetry instrumentation section. Tobs currently configures the OpenTelemetry Collector to ingest OpenTelemetry and Jaeger traces. Support for Zipkin traces will be available soon.

If you decide to use your own OpenTelemetry Collector you must enable the OTLP, Jaeger and/or Zipkin receivers based on the trace formats you want to use. You must also enable the OTLP exporter and point it to the Promscale OTLP endpoint. Promscale listend to OTLP data in the port you specified with the `otlp-grpc-server-listen-address` parameter (9202 if you followed the instructions in this document).

Below is an example of the OpenTelemetry Collector configuration file with the OTLP, Jaeger and Zipkin receivers enabled as well as the OTLP exporter. The example also enables the batch processor for improved efficiency when sending data to Promscale which we recommend.

```yaml
receivers:
  jaeger:
    protocols:
      grpc:
      thrift_http:

  otlp:
    protocols:
      grpc:
      http:

  zipkin:    

exporters:
  otlp:
    endpoint: "tobs-promscale-connector.default.svc.cluster.local:9202"
    insecure: true

processors:
  batch:
    send_batch_size: 4
    send_batch_max_size: 8

service:
  pipelines:
    traces:
      receivers: [jaeger, otlp, zipkin]
      exporters: [otlp]
      processors: [batch]
```

#### Troubleshooting

If you notice missing span data in Promscale, check the OpenTelemetry logs for errors.
If you see log lines similar to

```
2021-10-08T12:34:00.360Z        warn    batchprocessor/batch_processor.go:184   Sender failed   {"kind": "processor", "name": "batch", "error": "sending_queue is full"}
```

together with

```
2021-10-10T18:49:23.304Z        info    exporterhelper/queued_retry.go:325      Exporting failed. Will retry the request after interval.        {"kind": "exporter", "name": "otlp", "error": "failed to push trace data via OTLP exporter: rpc error: code = DeadlineExceeded desc = context deadline exceeded", "interval": "5.872756134s"}
```

try reducing the send_batch_size and send_batch_max_size of the [batch processor](https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/batchprocessor/README.md).

### OpenTelemetry instrumentation

If your service is instrumented with OpenTelemetry, configure the OpenTelemetry SDK to export your traces via the OTLP exporter to the OpenTelemetry Collector OTLP receiver (recommended) or to Promscale’s native OTLP ingest endpoint. The OTLP receiver supports both gRPC and HTTP. Promscale supports gRPC only. gRPC is recommended.

By default the OpenTelemetry Collector OTLP receiver listens on port 4317 for gRPC and 4318 for HTTP connections. If using gRPC you will configure the OTLP exporter to send data to `<opentelemetry-collector-host>:4317`. If you deployed a full observability stack via tobs use `tobs-opentelemetry-collector-collector.default.svc.cluster.local:4317`

Promscale’s OTLP ingest endpoint listens to gRPC connections on the address you specify with the `otlp-grpc-server-listen-address` parameter. If you followed the instructions provided in this document Promscale will be listening on port 9202 so you’ll have to point the OTLP exporter to `<promscale-connector-host>:9202`. If you deployed with tobs use `tobs-promscale-connector.default.svc.cluster.local:9202`.

### Jaeger instrumentation

If your service is instrumented with Jaeger, configure the Jaeger agent to send your traces to the OpenTelemetry Collector by passing [the reporter.grpc.host.port parameter](https://www.jaegertracing.io/docs/1.26/deployment/#discovery-system-integration) at start time with the host:port where the [OpenTelemetry Collector Jaeger Receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/jaegerreceiver) is listening for connections. By default the receiver listens for gRPC connections on port 14250. Therefore you should point the Jaeger agent to `<opentelemetry-collector-host>:14250`

If you used tobs to deploy your observability stack use tobs-opentelemetry-collector.default.svc.cluster.local:14250.

### Zipkin instrumentation

If your service is instrumented with Zipkin, configure the Zipkin transport you are using to send traces to the [OpenTelemetry Collector Zipkin Receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/zipkinreceiver) endpoint: `<opentelemetry-collector-host>:9411`

Tobs does not currently configure the OpenTelemetry Collector to ingest Zipkin traces (coming soon).

## Visualizing your traces in Promscale

### Instructions for Tobs

If you used tobs to install Promscale and also deployed all the other components it includes, everything is already set up for you to access [Grafana](https://github.com/grafana/grafana) and [Jaeger](https://github.com/jaegertracing/jaeger) to explore and visualize your traces.

To access the Jaeger UI from your machine run
```bash
tobs jaeger port-forward
```

Then, point your browser to [http://127.0.0.1:16686/] . No login credentials are required.

To access the Grafana user interface from your machine run

```bash
tobs grafana change-password <new_password>
tobs grafana port-forward
```

Then, point your browser to [http://127.0.0.1:8080/] and login with username `admin` and the password you specified in the previous step.

### Setting up Jaeger UI

In order for the Jaeger UI to show traces stored in Promscale we leverage Jaeger’s support for [gRPC storage plugins](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc). Our plugin acts as a simple proxy between Jaeger and Promscale. It does not contain any logic. All the processing work is done in the Promscale Connector.

This plugin only implements the APIs for Jaeger to read traces from Promscale. It does not implement the APIs for Jaeger to write traces to Promscale. To send Jaeger traces to Promscale use the OpenTelemetry Collector instead as explained [here](#jaeger-instrumentation).

Jaeger's gRPC plugin system works by executing the binary for the plugin when enabled in the configuration file. For that reason when deploying as a container, Jaeger and the binary need to be on the same container image. And since Jaeger doesn’t package all gRPC storage plugins in its default Docker images, we provide an image that includes the upstream Jaeger Query component (not the rest since they are not needed) and Promscale’s gRPC storage plugin for Jaeger. The image is available on [DockerHub](https://hub.docker.com/r/timescale/jaeger-query-proxy/tags). We recomment using the `latest` image

To enable Jaeger to use the plugin you need to pass the following parameters:

* `span-storage.type=grpc-plugin`
* `grpc-storage-plugin.binary=<path-to-jaeger-query-proxy-binary>`, pointing to the location of the plugin binary
* `grpc-storage-plugin.configuration-file=<config_file>`, a path pointing to the plugin's configuration file.

This is how you would run the container with Docker:

```bash
docker run --name promscale-jaeger -d -p 16686:16686 -v <path-to-plugin-config-file>:/configs/jaeger-promscale-query.yaml --network promscale-timescaledb timescale/jaeger-query-proxy:latest
```
The container already sets the required values for those parameters.

The Jaeger UI would be accessible on port 16686.

If you run Jaeger directly on a host, you first need to download the plugin binary for your system. The binaries are available under the assets of the latest 0.7 Promscale release on [Github](https://github.com/timescale/promscale/releases). Then you have to run the binary as follows:

```bash
./jaeger-query-plugin --span-storage.type=grpc-plugin --grpc-storage-plugin.binary=<path-to-jaeger-query-proxy-binary> --grpc-storage-plugin.configuration-file=<config_file>
```

The parameters in the plugin configuration file are the following (only the first is mandatory):

```yaml
grpc-server: <promscale-host>:<otlp-grpc-port>
#connection-timeout: 5s
#grpc-server-host-override: ""
#cafile: ""
#tls: false
```

If you followed the instructions described in this document then otlp-grpc-port will be 9202. For example

```yaml
grpc-server: localhost:9202
```

If you run on Kubernetes, create a ConfigMap like the one below

```yaml
​​apiVersion: v1
kind: ConfigMap
metadata:
  name: promscale-jaeger
data:
  jaeger-promscale-query.yaml: |
    grpc-server: <promscale-service>:9202   
```

Then make this ConfigMap available to the promscale-jaeger container through a volumeMount. Read more on how to do that in the [Kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/configmap/#configmaps-and-pods).

### Setting up Grafana

Grafana can query and visualize traces in Promscale through Jaeger. You’ll need Grafana version 7.4 or higher.

Go into Grafana and configure a Jaeger data source by passing the url of the Jaeger instance and credentials if you have enabled authentication in Jaeger. You have to specify the port you use to access the Jaeger UI which by default is 16686.

You can read more details on how to configure a Jaeger data source in the [Grafana documentation](https://grafana.com/docs/grafana/latest/datasources/jaeger/).

To access your traces go to Explore and select the Jaeger data source you just created. More details can be found in the [Grafana documentation](https://grafana.com/docs/grafana/latest/datasources/jaeger/).
