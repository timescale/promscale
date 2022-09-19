(import 'promscale.libsonnet')

{
  grafanaDashboards+:: {
    'apm-dependencies.json': (import 'apm-dependencies.json'),
    'apm-home.json': (import 'apm-home.json'),
    'apm-service-dependencies-downstream.json': (import 'apm-service-dependencies-downstream.json'),
    'apm-service-dependencies-upstream.json': (import 'apm-service-dependencies-upstream.json'),
    'apm-service-overview.json': (import 'apm-service-overview.json'),
  },
}
