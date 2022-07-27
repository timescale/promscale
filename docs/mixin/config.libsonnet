{
  // This is left to allow possible customization in the future
  _config+:: {
    dashboards: {
      promscale: {
        jobVariableSelector: '',
        instanceVariableSelector: '',

        enableMultiCluster: false,
        clusterVariableSelector: 'job=~"$job",instance=~"$instance"',
        namespaceVariableSelector: if self.enableMultiCluster then 'job=~"$job",instance=~"$instance",cluster="$cluster"' else 'job=~"$job",instance=~"$instance"',
        serviceVariableSelector: if self.enableMultiCluster then 'job=~"$job",instance=~"$instance",cluster="$cluster",namespace="$namespace"' else 'job=~"$job",instance=~"$instance",namespace="$namespace"',

        defaultSelector: if self.enableMultiCluster then 'cluster="$cluster",job=~"$job",instance=~"$instance",namespace="$namespace",service="$service"' else 'job=~"$job",instance=~"$instance",namespace="$namespace",service="$service"',

        buildInfoSelector: self.defaultSelector,
        ingestItemsTotalSelector: self.defaultSelector,
        sqlDatabaseCompressionStatusSelector: self.defaultSelector,
        ingestRequestsTotalSelector: self.defaultSelector,
        ingestRequestsTotalErrorSelector: self.defaultSelector + ',code=~"5.."',
        ingestDurationSecondsBucketSelector: self.defaultSelector,
        grpcServerHandledTotalSelector: self.defaultSelector + ',grpc_service=~"opentelemetry.proto.collector.trace.v1.TraceService",grpc_method=~"Export"',
        grpcServerHandledTotalErrorSelector: self.defaultSelector + ',grpc_service=~"opentelemetry.proto.collector.trace.v1.TraceService",grpc_code=~"Aborted|Unavailable|Internal|Unknown|Unimplemented|DataLoss",grpc_method=~"Export"',
        grpcServerHandlingSecondsBucketSelector: self.defaultSelector + ',grpc_service="opentelemetry.proto.collector.trace.v1.TraceService",grpc_method=~"Export"',
        queryRequestsTotalSelector: self.defaultSelector,
        queryRequestsTotalErrorSelector: self.defaultSelector + ',code=~"5.."',
        queryDurationSecondsBucketSelector: self.defaultSelector,
        sqlDatabaseWorkerMaintenanceJobFailedSelector: self.defaultSelector,
        sqlDatabaseChunksCompressedCountSelector: self.defaultSelector,
        sqlDatabaseHealthCheckErrorsTotalSelector: self.defaultSelector,
        sqlDatabaseHealthCheckTotalSelector: self.defaultSelector,
        sqlDatabaseWorkerMaintenanceJobStartTimestampSecondsSelector: self.defaultSelector,
        databaseRequestsDurationSecondsBucketQuerySelector: self.defaultSelector + ',method=~"query.*"',
        databaseRequestsDurationSecondsBucketNonQuerySelector: self.defaultSelector + ',method!~"query.*"',
        databaseRequestsTotalSelector: self.defaultSelector,
        sqlDatabaseNetworkLatencyMillisecondsSelector: self.defaultSelector,
        hitRatioMetricsSelector: self.defaultSelector + ',type="metric"',
        hitRatioTracesSelector: self.defaultSelector + ',type="trace"',
        cacheEvictionsTotalSelector: self.defaultSelector,
        cacheQueryLatencyMicrosecondsBucketMetricsSelector: self.defaultSelector + ',type="metric"',
        cacheQueryLatencyMicrosecondsBucketTracesSelector: self.defaultSelector + ',type="trace"',
        cacheOccupancySelector: self.defaultSelector,
      },
    },
  },
}
