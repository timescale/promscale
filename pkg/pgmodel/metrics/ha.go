package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/timescale/promscale/pkg/util"
)

var (
	HAClusterLeaderDetails = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ha",
			Name:      "cluster_leader_info",
			Help:      "Info on HA clusters and respective leaders.",
		},
		[]string{"cluster", "replica"})
	NumOfHAClusterLeaderChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: util.PromNamespace,
			Subsystem: "ha",
			Name:      "cluster_leader_changes_total",
			Help:      "Total number of times leader changed per cluster.",
		},
		[]string{"cluster"})
)

func init() {
	prometheus.MustRegister(HAClusterLeaderDetails, NumOfHAClusterLeaderChanges)
}
