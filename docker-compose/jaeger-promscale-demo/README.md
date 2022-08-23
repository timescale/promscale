# Hot R.O.D. - Rides on Demand

This is a demo application that consists of several microservices and illustrates
the use of the OpenTracing API. It can be run standalone, but requires Jaeger backend
to view the traces. A tutorial / walkthrough is available:
  * as a blog post [Take OpenTracing for a HotROD ride][hotrod-tutorial],
  * as a video [OpenShift Commons Briefing: Distributed Tracing with Jaeger & Prometheus on Kubernetes][hotrod-openshift].

## Features

* Discover architecture of the whole system via data-driven dependency diagram
* View request timeline & errors, understand how the app works
* Find sources of latency, lack of concurrency
* Highly contextualized logging
* Use baggage propagation to
  * Diagnose inter-request contention (queueing)
  * Attribute time spent in a service
* Use open source libraries with OpenTracing integration to get vendor-neutral instrumentation for free

## Running

### Run everything via `docker-compose`

* Download `docker-compose.yml` from https://github.com/timescale/promscale/blob/master/docker-compose/jaeger-promscale-demo/docker-compose.yaml
* Run Jaeger backend and HotROD demo with `docker-compose -f path-to-yml-file up`
* Access Jaeger UI at http://localhost:16686 and HotROD app at http://localhost:8080
* Shutdown / cleanup with `docker-compose -f path-to-yml-file down`

[hotrod-tutorial]: https://medium.com/@YuriShkuro/take-opentracing-for-a-hotrod-ride-f6e3141f7941
[hotrod-openshift]: https://blog.openshift.com/openshift-commons-briefing-82-distributed-tracing-with-jaeger-prometheus-on-kubernetes/
