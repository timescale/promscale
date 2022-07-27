{
  grafanaDashboards+:: {
    'promscale.json': {
      __elements: [],
      __requires: [
        {
          type: 'grafana',
          id: 'grafana',
          name: 'Grafana',
          version: '8.5.4',
        },
        {
          type: 'datasource',
          id: 'prometheus',
          name: 'Prometheus',
          version: '1.0.0',
        },
        {
          type: 'panel',
          id: 'stat',
          name: 'Stat',
          version: '',
        },
        {
          type: 'panel',
          id: 'table',
          name: 'Table',
          version: '',
        },
        {
          type: 'panel',
          id: 'timeseries',
          name: 'Time series',
          version: '',
        },
      ],
      annotations: {
        list: [
          {
            builtIn: 1,
            datasource: {
              type: 'datasource',
              uid: 'grafana',
            },
            enable: true,
            hide: true,
            iconColor: 'rgba(0, 211, 255, 1)',
            name: 'Annotations & Alerts',
            target: {
              limit: 100,
              matchAny: false,
              tags: [],
              type: 'dashboard',
            },
            type: 'dashboard',
          },
        ],
      },
      editable: true,
      fiscalYearStartMonth: 0,
      graphTooltip: 2,
      id: null,
      links: [],
      liveNow: false,
      panels: [
        {
          collapsed: true,
          datasource: {
            type: 'prometheus',
            uid: '-BqhIPC7z',
          },
          gridPos: {
            h: 1,
            w: 24,
            x: 0,
            y: 0,
          },
          id: 13,
          panels: [],
          title: 'Overview',
          type: 'row',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'thresholds',
              },
              custom: {
                align: 'auto',
                displayMode: 'auto',
                inspect: false,
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
            },
            overrides: [],
          },
          gridPos: {
            h: 5,
            w: 5,
            x: 0,
            y: 1,
          },
          id: 46,
          options: {
            footer: {
              fields: [],
              reducer: [
                'sum',
              ],
              show: true,
            },
            showHeader: true,
          },
          pluginVersion: '8.5.4',
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: false,
              expr: 'count by (version, branch, instance) (promscale_build_info{%(buildInfoSelector)s})' % $._config.dashboards.promscale,
              instant: true,
              interval: '',
              legendFormat: '',
              refId: 'A',
            },
          ],
          transformations: [
            {
              id: 'labelsToFields',
              options: {
                keepLabels: [
                  'version',
                ],
                mode: 'columns',
              },
            },
            {
              id: 'merge',
              options: {},
            },
            {
              id: 'organize',
              options: {
                excludeByName: {
                  Time: true,
                  Value: false,
                },
                indexByName: {
                  Time: 4,
                  Value: 3,
                  instance: 0,
                  job: 1,
                  version: 2,
                },
                renameByName: {
                  Value: '# of instances',
                  job: '',
                  version: '',
                },
              },
            },
          ],
          type: 'table',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 'wps',
            },
            overrides: [],
          },
          gridPos: {
            h: 8,
            w: 19,
            x: 5,
            y: 1,
          },
          id: 4,
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'sum by (kind) (rate(promscale_ingest_items_total{%(ingestItemsTotalSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '{{ kind }}',
              refId: 'A',
            },
          ],
          title: 'Samples Ingest Rate',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          description: 'Database compression status. If at least one promecale connector reports compression being disabled, this panel should turn red.',
          fieldConfig: {
            defaults: {
              color: {
                mode: 'thresholds',
              },
              mappings: [
                {
                  options: {
                    '0': {
                      color: 'red',
                      index: 2,
                      text: 'OFF',
                    },
                    '1': {
                      color: 'green',
                      index: 0,
                      text: 'ON',
                    },
                  },
                  type: 'value',
                },
                {
                  options: {
                    match: 'null+nan',
                    result: {
                      color: 'orange',
                      index: 1,
                      text: 'UNKNOWN',
                    },
                  },
                  type: 'special',
                },
              ],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                ],
              },
              unit: 'none',
            },
            overrides: [],
          },
          gridPos: {
            h: 3,
            w: 5,
            x: 0,
            y: 6,
          },
          id: 22,
          options: {
            colorMode: 'value',
            graphMode: 'none',
            justifyMode: 'center',
            orientation: 'auto',
            reduceOptions: {
              calcs: [
                'lastNotNull',
              ],
              fields: '',
              values: false,
            },
            textMode: 'auto',
          },
          pluginVersion: '8.5.4',
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'min(promscale_sql_database_compression_status{%(sqlDatabaseCompressionStatusSelector)s})' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '',
              refId: 'A',
            },
          ],
          title: 'Compression status',
          type: 'stat',
        },
        {
          collapsed: true,
          datasource: {
            type: 'prometheus',
            uid: '-BqhIPC7z',
          },
          gridPos: {
            h: 1,
            w: 24,
            x: 0,
            y: 9,
          },
          id: 2,
          panels: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 0,
                y: 10,
              },
              id: 6,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'sum by (type) (rate(promscale_ingest_requests_total{%(ingestRequestsTotalSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ type }}',
                  refId: 'A',
                },
              ],
              title: 'Requests (HTTP)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 8,
                y: 10,
              },
              id: 10,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'sum by (type) (rate(promscale_ingest_requests_total{%(ingestRequestsTotalErrorSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ type }}',
                  refId: 'A',
                },
              ],
              title: 'Errors (HTTP)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 's',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 16,
                y: 10,
              },
              id: 8,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(promscale_ingest_duration_seconds_bucket{%(ingestDurationSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: 'p50 {{ type }}',
                  refId: 'A',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.90, rate(promscale_ingest_duration_seconds_bucket{%(ingestDurationSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p90 {{ type }}',
                  refId: 'B',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.90, rate(promscale_ingest_duration_seconds_bucket{%(ingestDurationSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p95 {{ type }}',
                  refId: 'C',
                },
              ],
              title: 'Duration (HTTP)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 0,
                y: 19,
              },
              id: 9,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'rate(grpc_server_handled_total{%(grpcServerHandledTotalSelector)s}[$__rate_interval]) > 0' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ grpc_type }}',
                  refId: 'A',
                },
              ],
              title: 'Requests (gRPC)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 8,
                y: 19,
              },
              id: 7,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'rate(grpc_server_handled_total{%(grpcServerHandledTotalErrorSelector)s}[$__rate_interval])' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ grpc_code }}',
                  refId: 'A',
                },
              ],
              title: 'Errors (gRPC)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 's',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 16,
                y: 19,
              },
              id: 11,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(grpc_server_handling_seconds_bucket{%(grpcServerHandlingSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: 'p50 {{ type }}',
                  refId: 'A',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(grpc_server_handling_seconds_bucket{%(grpcServerHandlingSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p90 {{ type }}',
                  refId: 'B',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(grpc_server_handling_seconds_bucket{%(grpcServerHandlingSecondsBucketSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p95 {{ type }}',
                  refId: 'C',
                },
              ],
              title: 'Duration (gRPC)',
              type: 'timeseries',
            },
          ],
          title: 'Ingest',
          type: 'row',
        },
        {
          collapsed: true,
          datasource: {
            type: 'prometheus',
            uid: '-BqhIPC7z',
          },
          gridPos: {
            h: 1,
            w: 24,
            x: 0,
            y: 10,
          },
          id: 32,
          panels: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 0,
                y: 11,
              },
              id: 43,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'hidden',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'sum by (type) (rate(promscale_query_requests_total{%(queryRequestsTotalSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ type }}',
                  refId: 'A',
                },
              ],
              title: 'Requests (HTTP)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'reqps',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 8,
                y: 11,
              },
              id: 44,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'hidden',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'sum by (type) (rate(promscale_query_requests_total{%(queryRequestsTotalErrorSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ type }}',
                  refId: 'A',
                },
              ],
              title: 'Errors (HTTP)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 's',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 16,
                y: 11,
              },
              id: 45,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, sum by (le, instance, job) (rate(promscale_query_duration_seconds_bucket{%(queryDurationSecondsBucketSelector)s}[$__rate_interval])))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: 'p50',
                  refId: 'A',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.90, sum by (le, instance, job) (rate(promscale_query_duration_seconds_bucket{%(queryDurationSecondsBucketSelector)s}[$__rate_interval])))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p90',
                  refId: 'B',
                },
              ],
              title: 'Duration (HTTP)',
              type: 'timeseries',
            },
          ],
          title: 'Query',
          type: 'row',
        },
        {
          collapsed: true,
          datasource: {
            type: 'prometheus',
            uid: '-BqhIPC7z',
          },
          gridPos: {
            h: 1,
            w: 24,
            x: 0,
            y: 11,
          },
          id: 19,
          panels: [],
          title: 'Database',
          type: 'row',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'thresholds',
              },
              mappings: [
                {
                  options: {
                    '0': {
                      color: 'green',
                      index: 0,
                      text: 'OK',
                    },
                    '1': {
                      color: 'red',
                      index: 2,
                      text: 'FAILED',
                    },
                  },
                  type: 'value',
                },
                {
                  options: {
                    match: 'null+nan',
                    result: {
                      color: 'orange',
                      index: 1,
                      text: 'UNKNOWN',
                    },
                  },
                  type: 'special',
                },
              ],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                ],
              },
              unit: 'none',
            },
            overrides: [],
          },
          gridPos: {
            h: 4,
            w: 4,
            x: 0,
            y: 12,
          },
          id: 24,
          options: {
            colorMode: 'value',
            graphMode: 'none',
            justifyMode: 'center',
            orientation: 'auto',
            reduceOptions: {
              calcs: [
                'lastNotNull',
              ],
              fields: '',
              values: false,
            },
            textMode: 'auto',
          },
          pluginVersion: '8.5.4',
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'max(promscale_sql_database_worker_maintenance_job_failed{%(sqlDatabaseWorkerMaintenanceJobFailedSelector)s})' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '',
              refId: 'A',
            },
          ],
          title: 'Last maintenance job status',
          type: 'stat',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              max: 1,
              min: 0,
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 'percentunit',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 10,
            x: 4,
            y: 12,
          },
          id: 21,
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'min by (job, instance) (promscale_sql_database_chunks_compressed_count{%(sqlDatabaseChunksCompressedCountSelector)s})\n/\nmax by (job, instance)(promscale_sql_database_chunks_count{%(sqlDatabaseChunksCompressedCountSelector)s})' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '{{ instance }}',
              refId: 'A',
            },
          ],
          title: 'Compressed Chunks',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              max: 1,
              min: 0,
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 'percentunit',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 10,
            x: 14,
            y: 12,
          },
          id: 23,
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: '1 - promscale_sql_database_health_check_errors_total{%(sqlDatabaseHealthCheckErrorsTotalSelector)s} / promscale_sql_database_health_check_total{%(sqlDatabaseHealthCheckTotalSelector)s}' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '{{ instance }}',
              refId: 'A',
            },
          ],
          title: 'Database health',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'thresholds',
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                ],
              },
              unit: 'dateTimeAsIso',
            },
            overrides: [],
          },
          gridPos: {
            h: 5,
            w: 4,
            x: 0,
            y: 16,
          },
          id: 15,
          options: {
            colorMode: 'value',
            graphMode: 'none',
            justifyMode: 'center',
            orientation: 'auto',
            reduceOptions: {
              calcs: [
                'lastNotNull',
              ],
              fields: '',
              values: false,
            },
            textMode: 'auto',
          },
          pluginVersion: '8.5.4',
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'max(promscale_sql_database_worker_maintenance_job_start_timestamp_seconds{%(sqlDatabaseWorkerMaintenanceJobStartTimestampSecondsSelector)s}) * 1000' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '',
              refId: 'A',
            },
          ],
          title: 'Last DB maintenance job start',
          type: 'stat',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 's',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 12,
            x: 0,
            y: 21,
          },
          id: 27,
          interval: '2m',
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.5, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: 'p50 - {{ method }}',
              refId: 'A',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.9, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              hide: false,
              interval: '',
              legendFormat: 'p90 - {{ method }}',
              refId: 'B',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.95, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              hide: false,
              interval: '',
              legendFormat: 'p95 - {{ method }}',
              refId: 'C',
            },
          ],
          title: 'Duration (query)',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 's',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 12,
            x: 12,
            y: 21,
          },
          id: 28,
          interval: '2m',
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.5, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketNonQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: 'p50 - {{ method }}',
              refId: 'A',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.9, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketNonQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              hide: false,
              interval: '',
              legendFormat: 'p90 - {{ method }}',
              refId: 'B',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'histogram_quantile(0.95, rate(promscale_database_requests_duration_seconds_bucket{%(databaseRequestsDurationSecondsBucketNonQuerySelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
              hide: false,
              interval: '',
              legendFormat: 'p95 - {{ method }}',
              refId: 'C',
            },
          ],
          title: 'Duration (non-query)',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 'reqps',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 12,
            x: 0,
            y: 30,
          },
          id: 26,
          interval: '2m',
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              exemplar: true,
              expr: 'rate(promscale_database_requests_total{%(databaseRequestsTotalSelector)s}[$__rate_interval])' % $._config.dashboards.promscale,
              interval: '',
              legendFormat: '{{ method }}',
              refId: 'A',
            },
          ],
          title: 'Requests',
          type: 'timeseries',
        },
        {
          datasource: {
            type: 'prometheus',
            uid: '$datasource',
          },
          fieldConfig: {
            defaults: {
              color: {
                mode: 'palette-classic',
              },
              custom: {
                axisLabel: '',
                axisPlacement: 'auto',
                barAlignment: 0,
                drawStyle: 'line',
                fillOpacity: 0,
                gradientMode: 'none',
                hideFrom: {
                  legend: false,
                  tooltip: false,
                  viz: false,
                },
                lineInterpolation: 'linear',
                lineWidth: 1,
                pointSize: 5,
                scaleDistribution: {
                  type: 'linear',
                },
                showPoints: 'auto',
                spanNulls: false,
                stacking: {
                  group: 'A',
                  mode: 'none',
                },
                thresholdsStyle: {
                  mode: 'off',
                },
              },
              mappings: [],
              thresholds: {
                mode: 'absolute',
                steps: [
                  {
                    color: 'green',
                    value: null,
                  },
                  {
                    color: 'red',
                    value: 80,
                  },
                ],
              },
              unit: 'ms',
            },
            overrides: [],
          },
          gridPos: {
            h: 9,
            w: 12,
            x: 12,
            y: 30,
          },
          id: 48,
          options: {
            legend: {
              calcs: [],
              displayMode: 'list',
              placement: 'bottom',
            },
            tooltip: {
              mode: 'single',
              sort: 'none',
            },
          },
          targets: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              editorMode: 'code',
              expr: 'promscale_sql_database_network_latency_milliseconds{%(sqlDatabaseNetworkLatencyMillisecondsSelector)s}' % $._config.dashboards.promscale,
              legendFormat: '{{instance}}',
              range: true,
              refId: 'A',
            },
          ],
          title: 'Network latency',
          type: 'timeseries',
        },
        {
          collapsed: true,
          datasource: {
            type: 'prometheus',
            uid: '-BqhIPC7z',
          },
          gridPos: {
            h: 1,
            w: 24,
            x: 0,
            y: 39,
          },
          id: 30,
          panels: [
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  max: 1,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'percentunit',
                },
                overrides: [],
              },
              gridPos: {
                h: 8,
                w: 8,
                x: 0,
                y: 13,
              },
              id: 34,
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'promscale_cache_query_hits_total{%(hitRatioMetricsSelector)s} / promscale_cache_queries_total{%(hitRatioMetricsSelector)s}' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ name }}',
                  refId: 'A',
                },
              ],
              title: 'Hit ratio (metrics)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  max: 1,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'percentunit',
                },
                overrides: [],
              },
              gridPos: {
                h: 8,
                w: 8,
                x: 8,
                y: 13,
              },
              id: 35,
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'promscale_cache_query_hits_total{%(hitRatioTracesSelector)s} / promscale_cache_queries_total{%(hitRatioTracesSelector)s}' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ name }}',
                  refId: 'A',
                },
              ],
              title: 'Hit ratio (traces)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'cps',
                },
                overrides: [],
              },
              gridPos: {
                h: 8,
                w: 8,
                x: 16,
                y: 13,
              },
              id: 37,
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'sum by (type, name) (rate(promscale_cache_evictions_total{%(cacheEvictionsTotalSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ type }} - {{ name }}',
                  refId: 'A',
                },
              ],
              title: 'Evictions',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'ms',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 0,
                y: 21,
              },
              id: 41,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(promscale_cache_query_latency_microseconds_bucket{%(cacheQueryLatencyMicrosecondsBucketMetricsSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: 'p50 - {{ name }}',
                  refId: 'A',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.90, rate(promscale_cache_query_latency_microseconds_bucket{%(cacheQueryLatencyMicrosecondsBucketMetricsSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p90 - {{ name }}',
                  refId: 'B',
                },
              ],
              title: 'Latency (metrics)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  min: 0,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'ms',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 8,
                y: 21,
              },
              id: 42,
              interval: '2m',
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.5, rate(promscale_cache_query_latency_microseconds_bucket{%(cacheQueryLatencyMicrosecondsBucketTracesSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: 'p50 {{ name }}',
                  refId: 'A',
                },
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'histogram_quantile(0.90, rate(promscale_cache_query_latency_microseconds_bucket{%(cacheQueryLatencyMicrosecondsBucketTracesSelector)s}[$__rate_interval]))' % $._config.dashboards.promscale,
                  hide: false,
                  interval: '',
                  legendFormat: 'p90 {{ name }}',
                  refId: 'B',
                },
              ],
              title: 'Latency (traces)',
              type: 'timeseries',
            },
            {
              datasource: {
                type: 'prometheus',
                uid: '$datasource',
              },
              fieldConfig: {
                defaults: {
                  color: {
                    mode: 'palette-classic',
                  },
                  custom: {
                    axisLabel: '',
                    axisPlacement: 'auto',
                    barAlignment: 0,
                    drawStyle: 'line',
                    fillOpacity: 0,
                    gradientMode: 'none',
                    hideFrom: {
                      legend: false,
                      tooltip: false,
                      viz: false,
                    },
                    lineInterpolation: 'linear',
                    lineWidth: 1,
                    pointSize: 5,
                    scaleDistribution: {
                      type: 'linear',
                    },
                    showPoints: 'auto',
                    spanNulls: false,
                    stacking: {
                      group: 'A',
                      mode: 'none',
                    },
                    thresholdsStyle: {
                      mode: 'off',
                    },
                  },
                  mappings: [],
                  max: 1,
                  thresholds: {
                    mode: 'absolute',
                    steps: [
                      {
                        color: 'green',
                      },
                      {
                        color: 'red',
                        value: 80,
                      },
                    ],
                  },
                  unit: 'percentunit',
                },
                overrides: [],
              },
              gridPos: {
                h: 9,
                w: 8,
                x: 16,
                y: 21,
              },
              id: 39,
              options: {
                legend: {
                  calcs: [],
                  displayMode: 'list',
                  placement: 'bottom',
                },
                tooltip: {
                  mode: 'single',
                  sort: 'none',
                },
              },
              targets: [
                {
                  datasource: {
                    type: 'prometheus',
                    uid: '$datasource',
                  },
                  exemplar: true,
                  expr: 'promscale_cache_elements{%(cacheOccupancySelector)s} / promscale_cache_capacity_elements{%(cacheOccupancySelector)s}' % $._config.dashboards.promscale,
                  interval: '',
                  legendFormat: '{{ name }}',
                  refId: 'A',
                },
              ],
              title: 'Occupancy',
              type: 'timeseries',
            },
          ],
          title: 'Cache',
          type: 'row',
        },
      ],
      schemaVersion: 36,
      style: 'dark',
      tags: [],
      templating: {
        list: [
          {
            current: {
              selected: false,
              text: 'default',
              value: 'default',
            },
            hide: 0,
            includeAll: false,
            label: 'Prometheus',
            multi: false,
            name: 'datasource',
            options: [],
            query: 'prometheus',
            refresh: 1,
            regex: '',
            skipUrlSync: false,
            type: 'datasource',
          },
          {
            current: {
              selected: false,
              text: '',
              value: '',
            },
            datasource: {
              type: 'prometheus',
              uid: '$datasource',
            },
            definition: '',
            hide: 0,
            includeAll: true,
            multi: true,
            allValue: '.+',
            name: 'job',
            label: 'job',
            options: [],
            query: {
              query: 'label_values(promscale_build_info{%(jobVariableSelector)s}, job)' % $._config.dashboards.promscale,
              refId: 'Prometheus-job-Variable-Query',
            },
            refresh: 2,
            regex: '',
            skipUrlSync: false,
            sort: 0,
            tagValuesQuery: '',
            tagsQuery: '',
            type: 'query',
            useTags: false,
          },
          {
            current: {
              selected: false,
              text: '.+',
              value: '.+',
            },
            datasource: {
              type: 'prometheus',
              uid: '$datasource',
            },
            definition: '',
            hide: 0,
            includeAll: true,
            multi: true,
            allValue: '.+',
            name: 'instance',
            label: 'instance',
            options: [],
            query: {
              query: 'label_values(promscale_build_info{%(instanceVariableSelector)s}, instance)' % $._config.dashboards.promscale,
              refId: 'Prometheus-instance-Variable-Query',
            },
            refresh: 2,
            regex: '',
            skipUrlSync: false,
            sort: 0,
            tagValuesQuery: '',
            tagsQuery: '',
            type: 'query',
            useTags: false,
          },
          {
            current: {
              selected: false,
              text: '.+',
              value: '.+',
            },
            datasource: {
              type: 'prometheus',
              uid: '$datasource',
            },
            definition: '',
            hide: if $._config.dashboards.promscale.enableMultiCluster then 0 else 2,
            includeAll: false,
            multi: false,
            name: 'cluster',
            options: [],
            query: {
              query: 'label_values(promscale_build_info{%(clusterVariableSelector)s}, cluster)' % $._config.dashboards.promscale,
              refId: 'Prometheus-cluster-Variable-Query',
            },
            refresh: 2,
            regex: '',
            skipUrlSync: false,
            sort: 0,
            tagValuesQuery: '',
            tagsQuery: '',
            type: 'query',
            useTags: false,
          },
          {
            current: {
              selected: false,
              text: '',
              value: '',
            },
            datasource: {
              type: 'prometheus',
              uid: '$datasource',
            },
            definition: '',
            hide: 0,
            includeAll: false,
            multi: false,
            name: 'namespace',
            options: [],
            query: {
              query: 'label_values(promscale_build_info{%(namespaceVariableSelector)s}, namespace)' % $._config.dashboards.promscale,
              refId: 'Prometheus-namespace-Variable-Query',
            },
            refresh: 2,
            regex: '',
            skipUrlSync: false,
            sort: 0,
            tagValuesQuery: '',
            tagsQuery: '',
            type: 'query',
            useTags: false,
          },
          {
            current: {
              selected: false,
              text: '',
              value: '',
            },
            datasource: {
              type: 'prometheus',
              uid: '$datasource',
            },
            definition: '',
            hide: 0,
            includeAll: false,
            multi: false,
            name: 'service',
            options: [],
            query: {
              query: 'label_values(promscale_build_info{%(serviceVariableSelector)s}, service)' % $._config.dashboards.promscale,
              refId: 'Prometheus-service-Variable-Query',
            },
            refresh: 2,
            regex: '',
            skipUrlSync: false,
            sort: 0,
            tagValuesQuery: '',
            tagsQuery: '',
            type: 'query',
            useTags: false,
          },
        ],
      },
      time: {
        from: 'now-30m',
        to: 'now',
      },
      timepicker: {},
      timezone: '',
      title: 'Promscale',
      uid: 'IcOe3VPnz',
      version: 3,
      weekStart: '',
    },
  },
}
