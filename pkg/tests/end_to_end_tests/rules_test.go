package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	prometheus_config "github.com/prometheus/prometheus/config"
	"github.com/stretchr/testify/require"
	"github.com/timescale/promscale/pkg/log"

	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/rules"
	"github.com/timescale/promscale/pkg/runner"
)

func TestRecordingRules(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pgxcfg := conn.Conn().Config()
		cfg := runner.Config{
			Migrate:          false,
			StopAfterMigrate: false,
			UseVersionLease:  true,
			PgmodelCfg: pgclient.Config{
				AppName:                 pgclient.DefaultApp,
				Database:                *testDatabase,
				Host:                    pgxcfg.Host,
				Port:                    int(pgxcfg.Port),
				User:                    pgxcfg.User,
				Password:                pgxcfg.Password,
				SslMode:                 "allow",
				MaxConnections:          -1,
				WriteConnectionsPerProc: 1,
			},
		}
		conn.Release()
		client, err := runner.CreateClient(&cfg)
		require.NoError(t, err)

		ingestor := client.Ingestor()

		ts := generateLargeTimeseries()
		numInsertables, _, err := ingestor.Ingest(context.Background(), newWriteRequestWithTs(ts))
		require.NoError(t, err)
		require.NotEqual(t, uint64(0), numInsertables)

		manager, err := rules.NewManager(context.Background(), prometheus.DefaultRegisterer, "", client)
		require.NoError(t, err)

		err = manager.ApplyConfig(&prometheus_config.DefaultConfig)
		require.NoError(t, err)

		err = manager.Update(time.Second, []string{"../testdata/rules.yaml"}, nil, "")
		require.NoError(t, err)

		fmt.Println("going into run")
		manager.Run() // This will block.
	})
}

const (
	configuration = `global:
  scrape_interval: 20s
  evaluation_interval: 5s

alerting:
  alertmanagers:
  - static_configs:
    - targets:
      - 'localhost:9093'`
)

func TestSendingAlerts(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conn, err := db.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pgxcfg := conn.Conn().Config()
		cfg := runner.Config{
			Migrate:          false,
			StopAfterMigrate: false,
			UseVersionLease:  true,
			PgmodelCfg: pgclient.Config{
				AppName:                 pgclient.DefaultApp,
				Database:                *testDatabase,
				Host:                    pgxcfg.Host,
				Port:                    int(pgxcfg.Port),
				User:                    pgxcfg.User,
				Password:                pgxcfg.Password,
				SslMode:                 "allow",
				MaxConnections:          -1,
				WriteConnectionsPerProc: 1,
			},
		}
		conn.Release()
		client, err := runner.CreateClient(&cfg)
		require.NoError(t, err)

		manager, err := rules.NewManager(context.Background(), prometheus.DefaultRegisterer, "http://localhost:9201", client)
		require.NoError(t, err)

		promConfig, err := prometheus_config.Load(configuration, true, log.GetLogger())
		require.NoError(t, err)

		err = manager.ApplyConfig(promConfig)
		require.NoError(t, err)

		err = manager.Update(time.Second, []string{"../testdata/rules.yaml"}, nil, "http://localhost:9093")
		require.NoError(t, err)

		go func() {
			manager.Run() // This will block.
		}()
		err = manager.ApplyConfig(promConfig)
		require.NoError(t, err)
		time.Sleep(time.Hour)
	})
}
