# Writing data to Promscale

Promscale provides a remote write endpoint which Prometheus uses to ingest metric data. Documentation on the details of the endpoint can be found in Prometheus documentation:
https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations

Location of the endpoint is `http://{Promscale web URL and port}/write`.
Promscale URL and port depend on your specific setup and configuration. For example, if you are running it locally with the default port, the complete endpoint would be:
http://localhost:9201/write

This endpoint only supports the HTTP POST method.

The default format for this endpoint are protocol buffers but Promscale also supports a JSON streaming format and a text format which is detailed in the next sections.

If you are using Prometheus, simply configure `remote_write` to point to this endpoint and you are done. If, however, you are writing a custom application to push data to Promscale, keep reading.

## General time-series format rules

A time-series must always contain both labels and samples.

Labels are a collection of label name/value pairs with unique label names. Every request must contain a label named `__name__` which cannot have an empty value. All other labels are optional metadata.

A series entry always contains two things:
* An integer timestamp in milliseconds since epoch, i.e. 1970-01-01 00:00:00 UTC, excluding leap second, represented as required by Go's [ParseInt](https://golang.org/pkg/strconv/#ParseInt) function.
* Floating point number that represents the actual measured value.

## JSON streaming format

This format was introduced in Promscale to enable easier usage of the endpoint when ingesting metric data from 3rd party tools. It is not part of the `remote_write` specification for Prometheus. It is slightly less efficient to use this format than the Protobuf format.

This format is a JSON stream of objects with two fields:
* `labels` JSON object in which all the labels are represented as fields
* `samples` JSON array which contains arrays of timestamp-value sets

Here is a short example of what a simple payload would look like:

```
{
    "labels":{"__name__": "cpu_usage", "namespace":"dev", "node": "brain"},
    "samples":[
        [1577836800000, 100],
        [1577836801000, 99],
        [1577836802000, 98],
        ...
    ]
}
{
    "labels":{"__name__": "cpu_usage", "namespace":"prod", "node": "brain"},
    "samples":[
        [1577836800000, 98],
        [1577836801000, 99],
        [1577836802000, 100],
        ...
    ]
}
...
```

As you can see, the labels object is pretty self explanatory. Fields represent the label name and value is the label value. Samples array consists of arrays with some strict rules:
* There can be only two values in the array
* First value is a UNIX timestamp in milliseconds which has to be an integer value
* Second value is a floating point number which represents the corresponding value of the metric.

In order to send a request to Promscale, you would need to send an HTTP POST request with the request body set to the JSON payload and set the required header values:
* `Content-Type` header should be set to `application/json`
* If using Snappy compression set `Content-Encoding` header to `snappy`, otherwise leave unset

Here is an example of a request sent using `curl` tool:

```
curl --header "Content-Type: application/json" \
--request POST \
--data '{"labels":{"__name__":"foo"},"samples":[[1577836800000, 100]]}' \
"http://localhost:9201/write"
```

Another example using snappy encoding:

```
curl --header "Content-Type: application/json" \
--header "Content-Encoding: snappy" \
--request POST \
--data-binary "@snappy-payload.sz" \
"http://localhost:9201/write"
```

## Protobuf format

This format is a little more complex and involved to use than the JSON format but generally is a bit more performant because the payload is smaller in size. It is recommended that you use this format if you are building a system which would ingest a lot of data continuously.

In order to use the endpoint, there are a few steps that need to be done:
* Fetch the protocol buffer definition files [here](https://github.com/prometheus/prometheus/blob/master/prompb/)
* [Compile them](https://developers.google.com/protocol-buffers/docs/tutorials) in into structures of the programming language you intend to use
* Finally, use those structures to construct requests which you can then send to the Promscale write endpoint
  Next section will show a simple example of how to make a request to Promscale using the Go programming language.

## Protobuf write request example in Go

The write protocol uses a snappy-compressed protocol buffer encoding over HTTP. Protocol buffer definition files can be found in the Prometheus codebase: https://github.com/prometheus/prometheus/blob/master/prompb/

These files are used to compile them into necessary classes for payload serialization. More details on this process can be found here:
https://developers.google.com/protocol-buffers/docs/tutorials

> Since Prometheus is written in Go language, it's also possible to import their generated classes from the [prompb](https://github.com/prometheus/prometheus/blob/master/prompb/) package in Prometheus repository.

Once you have completed this step, you should have the necessary classes to create a write request. More specifically, we are going to need the `WriteRequest` object which we will preload with metrics data, serialize it and send as an HTTP request.

A simplified version of the `main.go` file would look something like this:

```
package main

import (
    "bytes"
    "net/http"

    "github.com/gogo/protobuf/proto"
    "github.com/golang/snappy"
)

func main() {
    // Create the WriteRequest object with metric data populated.
    wr := &WriteRequest{
        Timeseries: []TimeSeries{
            TimeSeries{
                Labels: []Label{
                    {
                        Name:  "__name__",
                        Value: "foo",
                    },
                },
                Samples: []Sample{
                    {
                        Timestamp: 1577836800000,
                        Value:         100.0,
                    },
                },
            },
        },
    }

    // Marshal the data into a byte slice using the protobuf library.
    data, err := proto.Marshal(wr)
    if err != nil {
        panic(err)
    }

    // Encode the content into snappy encoding.
    encoded := snappy.Encode(nil, data)
    body := bytes.NewReader(encoded)

    // Create an HTTP request from the body content and set necessary parameters.
    req, err := http.NewRequest("POST", "http://localhost:9201/write", body)
    if err != nil {
        panic(err)
    }

    // Set the required HTTP header content.
    req.Header.Set("Content-Type", "application/x-protobuf")
    req.Header.Set("Content-Encoding", "snappy")
    req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

    // Send request to Promscale.
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        panic(err)
    }

    defer resp.Body.Close()
    ...
```

As you can see, once the Go code is generated from the protobuf files, you have everything you need to start putting your data into the generated structures and start sending requests to Promscale for ingestion.

## Prometheus/OpenMetric text format

This format was introduced in Promscale to enable easier ingestion of samples data using a push model. Metrics exposed in this format can be directly forwarded to Promscale which would parse and store the data in the database.
Advanced details on the format can be found here:
* [Prometheus text based format docs](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).
* [OpenMetrics format spec] (https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md)

Here is a short example of what a simple payload would look like:

```
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400"}    3 1395066363000

# A histogram, which has a pretty complex representation in the text format:
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.2"} 100392
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320
...
```

Basic line format for non-metadata is as follows (in this order):
* Metric name which is (optionally) followed by a collection label name and values in curly braces.
* Floating point number that represents the actual measured value.
* (Optional) An integer timestamp in milliseconds since epoch, i.e. 1970-01-01 00:00:00 UTC, excluding leap second, represented as required by Go's [ParseInt](https://golang.org/pkg/strconv/#ParseInt) function.

If the timestamp is omitted, request time is used in its place.

In order to send a request to Promscale, you would need to send an HTTP POST request with the request body set to the plain-text payload and set the required header values:
* `Content-Type` header should be set to `text/plain` or `application/openmetrics-text` (depending on the actual format).
* If using Snappy compression set `Content-Encoding` header to `snappy`, otherwise leave unset

Here is an example of a request sent using `curl` tool:

```
curl --header "Content-Type: text/plain" \
--request POST \
--data 'test_metric 1\nanother_metric 2' \
"http://localhost:9201/write"
```

Another example using snappy encoding:

```
curl --header "Content-Type: application/openmetrics-text" \
--header "Content-Encoding: snappy" \
--request POST \
--data-binary "@snappy-payload.sz" \
"http://localhost:9201/write"
```
