// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package integration

type testQuery struct {
	name       string
	isRule     bool
	expression string
}

var customQueries = []testQuery{
	// Real-world inspired PromQL queries.
	// References:
	// 1. https://www.robustperception.io/common-query-patterns-in-promql
	// 2. https://github.com/infinityworks/prometheus-example-queries
	//
	// Note: All metrics used here are non-rules, hence consider realStart and realEnd for range queries.
	{
		name:       "counter",
		expression: "container_fs_reads_total",
	}, {
		name:       "gauge",
		expression: "go_goroutines",
	}, {
		name:       "irate",
		expression: "irate(container_fs_reads_total[1m])",
	}, {
		name:       "rate short",
		expression: "rate(container_fs_reads_total[1m])",
	}, {
		name:       "rate long",
		expression: "rate(container_fs_reads_total[1h])",
	}, {
		name:       "rate on subquery",
		expression: "rate(container_fs_reads_total[1h:5m])",
	}, {
		name:       "sum",
		expression: "sum(go_goroutines)",
	}, {
		name:       "sum by",
		expression: "sum by (job) (go_goroutines)",
	}, {
		name:       "sum without",
		expression: "sum without (job) (go_goroutines)",
	}, {
		name:       "avg",
		expression: "avg(go_goroutines)",
	}, {
		name:       "avg by",
		expression: "avg by (job) (go_goroutines)",
	}, {
		name:       "avg without",
		expression: "avg without (job) (go_goroutines)",
	}, {
		name:       "max",
		expression: "max(go_goroutines)",
	}, {
		name:       "summary type",
		expression: "sum without (job) (rate(process_cpu_seconds_total[5m])) / sum without (job) (rate(container_fs_writes_total[5m]))",
	}, {
		name:       "utilization percentage",
		expression: "100 * (1 - avg by(instance)(irate(go_goroutines{foo='bar'}[5m])))",
	}, {
		name: "event occurange percentage (like rate of errors)",
		expression: `
	rate(container_fs_writes_total[5m]) * 50
> on(job, instance)
	rate(container_fs_writes_total[5m])`,
	}, {
		name: "calculate 90th percentile",
		expression: `
		histogram_quantile(
			0.9,
			sum without (instance)(rate(workqueue_queue_duration_seconds_bucket[5m])))`,
	}, {
		name: "percentile calculation 2",
		expression: `
	histogram_quantile(0.9, rate(rest_client_request_duration_seconds_bucket[10m])) > 0.05
and
	rate(rest_client_requests_total[10m]) > 1`,
	}, {
		name:       "request rate per second an hour ago",
		expression: `rate(kube_pod_container_resource_requests[5m] offset 1h)`,
	}, {
		name:       "top 5 most expensive time-series",
		expression: `topk(10, count by (__name__)({__name__=~".+"}))`,
	}, {
		name:       "alert 1: predict linear",
		expression: `predict_linear(process_cpu_seconds_total[4h], 4 * 3600) < 0`,
	}, {
		name:       "alert 2: high utilization",
		expression: `100 - (avg by(instance) (irate(container_fs_writes_total[5m])) * 100) > 70`,
	}, {
		name:       "alert 3: simple alert",
		expression: `workqueue_adds_total > 100`,
	},
}

var (
	realStart     = int64(1613038206590)
	realStartRule = int64(1631099235000)
	realEnd       = int64(1613051925000)
	realEndRule   = int64(1631100540000)
)

var realQueries = []testQuery{
	{
		name:       "simple gauge 1: percent of request in last 30 days answered successfully",
		expression: `apiserver_request:availability30d`,
		isRule:     true,
	}, {
		name:       "simple gauge 3: percent of request in last 30 days answered successfully",
		expression: `apiserver_request:availability30d{verb="all"}`,
		isRule:     true,
	}, {
		name:       "simple gauge 2: percent of request in last 30 days answered successfully",
		expression: `apiserver_request:availability30d{verb=~".+"}`,
		isRule:     true,
	}, {
		name:       "simple gauge 3: percent of request in last 30 days answered successfully",
		expression: `apiserver_request:availability30d{verb!=""}`,
		isRule:     true,
	}, {
		name:       "binary: error left at 0.990% availability",
		expression: `100 * (apiserver_request:availability30d{verb="all"} - 0.990000)`,
		isRule:     true,
	}, {
		name:       "sum: total read requests per second to apiserver",
		expression: `sum by (code) (code_resource:apiserver_request_total:rate5m{verb="read"})`,
		isRule:     true,
	}, {
		name:       "sum + binary: percent of read requests per second returned with errors",
		expression: `sum by (resource) (code_resource:apiserver_request_total:rate5m{verb="read",code=~"5.."}) / sum by (resource) (code_resource:apiserver_request_total:rate5m{verb="read"})`,
		isRule:     true,
	}, {
		name:       "histogram quantile: 99th percentile for reading a resource in seconds",
		expression: `cluster_quantile:apiserver_request_duration_seconds:histogram_quantile{verb="read"}`,
		isRule:     true,
	}, {
		name:       "sum + binary: percent write request per second returned with errors",
		expression: `sum by (resource) (code_resource:apiserver_request_total:rate5m{verb="write",code=~"5.."}) / sum by (resource) (code_resource:apiserver_request_total:rate5m{verb="write"})`,
		isRule:     true,
	}, {
		name:       "sum + rate: rate work queue addition",
		expression: `sum(rate(workqueue_adds_total[5m])) by (instance, name)`,
	}, {
		name:       "histogram + sum + rate: 0.99 quantile of work queue bucket duration",
		expression: `histogram_quantile(0.99, sum(rate(workqueue_queue_duration_seconds_bucket[5m])) by (instance, name, le))`,
	}, {
		name:       "rate: process cpu seconds",
		expression: `rate(process_cpu_seconds_total{job="kubernetes-apiservers"}[5m])`,
	}, {
		name:       "sort + sum + irate (regex all): total container network bytes",
		expression: `sort_desc(sum(irate(container_network_receive_bytes_total{namespace=~".+"}[5m])) by (namespace))`,
	}, {
		name:       "sort + avg + irate (regex all): total container network bytes",
		expression: `sort_desc(avg(irate(container_network_receive_bytes_total{namespace=~".+"}[5m])) by (namespace))`,
	}, {
		name:       "sort + sum + rate + subquery + binary: rate of retrans segs by rate of out segs",
		expression: `sort_desc(sum(rate(node_netstat_Tcp_RetransSegs{namespace="savannah-system"}[10m:1m]) / rate(node_netstat_Tcp_OutSegs{namespace="savannah-system"}[10m:1m])) by (instance))`,
	}, {
		name:       "sum + rate: work queues adds total",
		expression: `sum(rate(workqueue_adds_total[5m])) by (cluster, instance, name)`,
	}, {
		name:       "histogram quantile + sum + rate: 99th percentile of work queue duration",
		expression: `histogram_quantile(0.99, sum(rate(workqueue_queue_duration_seconds_bucket{instance="172.20.113.72:443"}[5m])) by (cluster, instance, name, le))`,
	}, {
		name:       "sum + rate: rest client request 2xx",
		expression: `sum(rate(rest_client_requests_total{code=~"2.."}[5m]))`,
	}, {
		name:       "sum + rate: rest client request 5xx",
		expression: `sum(rate(rest_client_requests_total{code=~"5.."}[5m]))`,
	}, {
		name:       "histogram quantile + sum + rate: client request duration",
		expression: `histogram_quantile(0.99, sum(rate(rest_client_request_duration_seconds_bucket{verb="GET"}[5m])) by (verb, url, le))`,
	}, {
		name:       "rate: process cpu seconds for kube-controller-manager",
		expression: `rate(process_cpu_seconds_total[5m])`,
	}, {
		name:       "simple gauge: go_goroutines",
		expression: `go_goroutines`,
	}, {
		name:       "simple gauge: go_goroutines + labelset",
		expression: `go_goroutines{container="cluster-autoscaler"}`,
	}, {
		name:       "call + call: count of avg workload on pod",
		expression: `count(avg(namespace_workload_pod:kube_pod_owner:relabel) by (workload, namespace)) by (namespace)`,
		isRule:     true,
	}, {
		name:       "call + call: network transmit bytes",
		expression: `sum(irate(container_network_transmit_bytes_total{namespace=~".+"}[5m])) by (namespace)`,
	}, {
		name:       "call + call: network receive bytes",
		expression: `avg(irate(container_network_transmit_bytes_total{namespace=~".+"}[5m])) by (namespace)`,
	}, {
		name:       "call + binary + sum: query 1",
		expression: `ceil(sum by(namespace) (rate(container_fs_reads_total{container!=""}[5m]) + rate(container_fs_writes_total{container!=""}[5m])))`,
	}, {
		name:       "call + call: query 2",
		expression: `sum by(namespace) (rate(container_fs_reads_total{container!=""}[5m]) + rate(container_fs_writes_total{container!=""}[5m]))`,
	}, {
		name:       "binary + call + call: query 4",
		expression: `sum by(namespace) (rate(container_fs_reads_bytes_total{container!=""}[5m]) + rate(container_fs_writes_bytes_total{container!=""}[5m]))`,
	}, {
		name:       "call: scalar val, query 5",
		expression: `scalar(kubelet_pod_worker_duration_seconds_sum)`,
	}, {
		name:       "call + binary + call + call: query 7",
		expression: `ceil(sum by(pod) (rate(container_fs_reads_total{container!=""}[5m]) + rate(container_fs_writes_total{container!=""}[5m])))`,
	}, {
		name:       "call + call + binary + call + call: query 8",
		expression: `sum(increase(container_cpu_cfs_throttled_periods_total{namespace="kube-system"}[5m])) by (container) /sum(increase(container_cpu_cfs_periods_total{namespace="kube-system"}[5m])) by (container)`,
	}, {
		name:       "call + call: query 9",
		expression: `sum(irate(container_network_receive_packets_total{job="kubernetes-cadvisor"}[5m])) by (pod)`,
	}, {
		name:       "call + binary + call: query 10",
		expression: `ceil(sum by(container) (rate(container_fs_reads_total{container=~"bill.+"}[5m]) + rate(container_fs_writes_total{container=~"bill.+"}[5m])))`,
	}, {
		// Empty result since node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate is non-existent.
		name: "binary + call + binary + binary + call: query 13",
		expression: `sum(
			node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate
	  * on(namespace,pod)
	      group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel
		  ) by (pod)
	  / sum(
		    kube_pod_container_resource_requests
		* on(namespace,pod) group_left(workload, workload_type) namespace_workload_pod:kube_pod_owner:relabel
			) by (pod)`,
		isRule: true,
	}, {
		name: "paren + binary + call + call: query 14",
		expression: `(sum(irate(container_network_receive_bytes_total[5m])
		* on (namespace,pod) group_left(workload,workload_type)
			namespace_workload_pod:kube_pod_owner:relabel) by (pod))`,
		isRule: true,
	}, {
		name: "paren + binary + call + call: query 15",
		expression: `(sum(irate(container_network_receive_packets_dropped_total[5m])
		* on (namespace,pod) group_left(workload,workload_type)
			namespace_workload_pod:kube_pod_owner:relabel) by (pod))`,
		isRule: true,
	}, {
		name: "binary + call + binary + binary + call: query 16",
		expression: `sum(
			node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate
		* on(namespace,pod) group_left(workload, workload_type)
			namespace_workload_pod:kube_pod_owner:relabel
			) by (workload, workload_type)
		/ 		sum(kube_pod_container_resource_requests
			* on(namespace,pod) group_left(workload, workload_type)
				namespace_workload_pod:kube_pod_owner:relabel)
				by (workload, workload_type)`,
		isRule: true,
	}, {
		name: "",
		expression: `(avg(irate(container_network_receive_bytes_total[10m])
		* on (namespace,pod) group_left(workload,workload_type)
			namespace_workload_pod:kube_pod_owner:relabel)
		  by (workload))`,
		isRule: true,
	}, {
		name:       "binary logical + call + call: query 18",
		expression: `sum(kubelet_running_pods) OR sum(kubelet_running_pod_count)`,
	}, {
		name:       "binary logical + call + call: query 19",
		expression: `sum(kubelet_running_containers) OR sum(kubelet_running_container_count)`,
	}, {
		name:       "call + call: query 20",
		expression: `sum(rate(kubelet_runtime_operations_total[5m])) by (operation_type, instance)`,
	}, {
		name:       "histogram + call + call: query 21",
		expression: `histogram_quantile(0.99, sum(rate(kubelet_runtime_operations_duration_seconds_bucket{operation_type="status"}[5m])) by (instance, operation_type, le))`,
	}, {
		name:       "histogram + call + call: query 22",
		expression: `histogram_quantile(0.99, sum(rate(storage_operation_duration_seconds_bucket[5m])) by (instance, operation_type, le))`,
	}, {
		name: "binary + call + call + call: query 23",
		expression: `sort_desc(sum(irate(container_network_receive_bytes_total[5m:30s])
		* on (namespace,pod) group_left(workload,workload_type)
			namespace_workload_pod:kube_pod_owner:relabel)
		  by (workload))`,
		isRule: true,
	}, {
		name: "paren + binary + call + call + paren + call + call + paren: query 24",
		expression: `(
			sum without(instance, node) (
					topk(1, (kubelet_volume_stats_capacity_bytes)))
			  -
				  sum without(instance, node) (
					  topk(1, (kubelet_volume_stats_available_bytes))))`,
	}, {
		name: "binary + call + binary + call + call + binary + call + scalar: query 25",
		expression: `max without(instance,node) (
			(
				topk(1, kubelet_volume_stats_capacity_bytes)
			-
				topk(1, kubelet_volume_stats_available_bytes)
			)
		/
				topk(1, kubelet_volume_stats_capacity_bytes)
			*
				100)`,
	}, {
		name: "paren + binary + call + call + call + call: query 26",
		expression: `(
			sum without(instance, node) (
				topk(10, (kubelet_container_log_filesystem_used_bytes)))
			-
				sum without(instance, node) (
					topk(1, (kubelet_container_log_filesystem_used_bytes))))`,
	}, {
		name:       "call + call: query 27",
		expression: `sum(rate(kubelet_docker_operations_errors_total[5m]))`,
	}, {
		name: "histogram + call + call: query 27",
		expression: `histogram_quantile(0.99,
			sum(rate(apiserver_watch_events_sizes_bucket[5m])) by (instance, le)
		)`,
	}, {
		name:       "histogram + call + call: query 28",
		expression: `histogram_quantile(0.99, sum(rate(rest_client_request_duration_seconds_bucket{verb="POST"}[5m])) by (verb, url, le))`,
	}, {
		name: "binary + call + call + call: query 29",
		expression: `sort_desc(avg(irate(container_network_transmit_bytes_total[5m:1m])
		* on (namespace,pod) group_left(workload,workload_type)
			namespace_workload_pod:kube_pod_owner:relabel)
			by (pod))`,
		isRule: true,
	},
}
