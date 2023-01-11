package database

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/timescale/promscale/pkg/internal/day"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/util"
)

var (
	caggsRefreshTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "sql_database",
		Name:      "caggs_refresh_total",
		Help:      "Total number of caggs policy executed.",
	}, []string{"refresh_interval"})
	caggsRefreshSuccess = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: util.PromNamespace,
		Subsystem: "sql_database",
		Name:      "caggs_refresh_success",
		Help:      "Total number of caggs policy executed successfully.",
	}, []string{"refresh_interval"})
)

func init() {
	prometheus.MustRegister(caggsRefreshSuccess, caggsRefreshTotal)
}

type metricsWithSeries struct {
	update func(conn pgxconn.PgxConn) error
}

var metricSeries = []metricsWithSeries{
	{
		update: func(conn pgxconn.PgxConn) error {
			rows, err := conn.Query(context.Background(), `
SELECT
    total_successes,
    total_runs,
    (config ->> 'refresh_interval')::INTERVAL
FROM timescaledb_information.jobs j
INNER JOIN timescaledb_information.job_stats js ON ( j.job_id = js.job_id AND j.proc_name = 'execute_caggs_refresh_policy')
			`)
			if err != nil {
				return fmt.Errorf("error running instrumentation for execute_caggs_refresh_policy: %w", err)
			}
			defer rows.Close()
			for rows.Next() {
				var (
					success, total  int64
					refreshInterval time.Duration
				)
				err = rows.Scan(&success, &total, &refreshInterval)
				if err != nil {
					return fmt.Errorf("error scanning values for execute_caggs_refresh_policy: %w", err)
				}
				tmp := day.Duration(refreshInterval) // This allows label values to have 25h -> 1d1h, which is easier to understand and matches more to the user's original input.
				caggsRefreshSuccess.With(prometheus.Labels{"refresh_interval": tmp.String()}).Set(float64(success))
				caggsRefreshTotal.With(prometheus.Labels{"refresh_interval": tmp.String()}).Set(float64(total))
			}
			return nil
		},
	},
}
