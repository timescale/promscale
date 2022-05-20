// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/route"
	"github.com/stretchr/testify/require"

	// Following imports for running alertmanager server.
	"github.com/prometheus/alertmanager/api"
	amConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/types"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgclient"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/query"
	"github.com/timescale/promscale/pkg/rules"
	"github.com/timescale/promscale/pkg/tenancy"
)

const (
	alertsConfigPath = "../testdata/rules/config.alerts.yaml"
	amConfigPath     = "../testdata/rules/config.alertmanager.yaml"
)

func TestAlerts(t *testing.T) {
	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		conf := &pgclient.Config{
			CacheConfig:             cache.DefaultConfig,
			WriteConnectionsPerProc: 4,
			MaxConnections:          -1,
		}

		pgClient, err := pgclient.NewClientWithPool(conf, 1, db, tenancy.NewNoopAuthorizer(), false)
		require.NoError(t, err)
		defer pgClient.Close()
		err = pgClient.InitPromQLEngine(&query.Config{
			MaxQueryTimeout:      query.DefaultQueryTimeout,
			EnabledFeatureMap:    map[string]struct{}{"promql-at-modifier": {}},
			SubQueryStepInterval: query.DefaultSubqueryStepInterval,
			LookBackDelta:        query.DefaultLookBackDelta,
			MaxSamples:           query.DefaultMaxSamples,
			MaxPointsPerTs:       11000,
		})
		require.NoError(t, err)

		rulesCfg := &rules.Config{
			NotificationQueueCapacity: rules.DefaultNotificationQueueCapacity,
			OutageTolerance:           rules.DefaultOutageTolerance,
			ForGracePeriod:            rules.DefaultForGracePeriod,
			ResendDelay:               time.Millisecond * 100,
			PrometheusConfigAddress:   alertsConfigPath,
		}
		require.NoError(t, rules.Validate(rulesCfg))
		require.True(t, rulesCfg.ContainsRules())

		manager, err := rules.NewManager(context.Background(), prometheus.NewRegistry(), pgClient, rulesCfg)
		require.NoError(t, err)

		require.NotNil(t, rulesCfg.PrometheusConfig)
		require.NoError(t, manager.ApplyConfig(rulesCfg.PrometheusConfig))
		require.NoError(t, manager.Update(time.Millisecond*100, rulesCfg.PrometheusConfig.RuleFiles, nil, ""))

		stopManager := make(chan struct{})
		go func() {
			go func() {
				require.NoError(t, manager.Run(), "error running rules manager")
			}()
			<-stopManager
			manager.Stop()
		}()

		// Start alerts receiver.
		alertReceived := make(chan bool)
		stopAM := make(chan struct{})
		alertsReceiver(t, stopAM, alertReceived)

		success := <-alertReceived // Wait for the expected alert to receive.
		require.True(t, success, "did not get expected alert")

		stopAM <- struct{}{} // Stop AM.
		<-stopAM             // Wait for AM to shutdown.

		close(stopManager)
		close(alertReceived) // This should always be called after AM is stopped, otherwise it may cause a panic.
	})
}

// alertsReceiver looks for alert requests by reusing modules from alertmanager.
// Most of the implementation is coped from
// https://github.com/prometheus/alertmanager/blob/f958b8be84b870e363f7dafcbeb807b463269a75/cmd/alertmanager/main.go#L186
func alertsReceiver(t testing.TB, stop chan struct{}, alertReceived chan<- bool) {
	f, err := ioutil.TempFile(os.TempDir(), "*.alerts.test")
	require.NoError(t, err)

	silences, err := silence.New(silence.Options{
		SnapshotFile: f.Name(),
		Retention:    time.Hour * 120,
		Logger:       log.GetLogger(),
		Metrics:      prometheus.DefaultRegisterer,
	})
	require.NoError(t, err)

	var disp *dispatch.Dispatcher
	groupFn := func(routeFilter func(*dispatch.Route) bool, alertFilter func(*types.Alert, time.Time) bool) (dispatch.AlertGroups, map[model.Fingerprint][]string) {
		return disp.Groups(routeFilter, alertFilter)
	}

	apiAM, err := api.New(api.Options{
		Alerts:     newFakeAlerts([]*types.Alert{}, false, alertReceived),
		Silences:   silences,
		StatusFunc: types.NewMarker(prometheus.NewRegistry()).Status,
		Logger:     log.GetLogger(),
		Registry:   prometheus.NewRegistry(),
		GroupFunc:  groupFn,
	})
	require.NoError(t, err)

	conf, err := amConfig.LoadFile(amConfigPath)
	require.NoError(t, err)
	apiAM.Update(conf, func(model.LabelSet) {})

	u, err := url.Parse("http://localhost:8080")
	require.NoError(t, err)

	router := route.New()
	mux := apiAM.Register(router, u.Path)
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		go func() {
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				log.Warn("msg", "unexpected error serving alert requests", "err", err.Error())
			}
		}()
		<-stop // Wait for test to complete before shutdown.
		require.NoError(t, srv.Shutdown(context.Background()))
		disp.Stop()
		require.NoError(t, os.RemoveAll(f.Name()))
		stop <- struct{}{}
	}()
}

// copied from
// https://github.com/prometheus/alertmanager/blob/f958b8be84b870e363f7dafcbeb807b463269a75/api/v1/api_test.go#L38
//
// fakeAlerts is a struct implementing the provider.Alerts interface for tests.
type fakeAlerts struct {
	fps     map[model.Fingerprint]int
	alerts  []*types.Alert
	success chan<- bool
	err     error
}

func newFakeAlerts(alerts []*types.Alert, withErr bool, sigSuccess chan<- bool) *fakeAlerts {
	fps := make(map[model.Fingerprint]int)
	for i, a := range alerts {
		fps[a.Fingerprint()] = i
	}
	f := &fakeAlerts{
		alerts:  alerts,
		fps:     fps,
		success: sigSuccess,
	}
	if withErr {
		f.err = fmt.Errorf("error occurred")
	}
	return f
}

func (f *fakeAlerts) Subscribe() provider.AlertIterator { return nil }
func (f *fakeAlerts) Get(model.Fingerprint) (*types.Alert, error) {
	return nil, nil
}
func (f *fakeAlerts) Put(alerts ...*types.Alert) error {
	f.success <- alerts[0].Name() == "Test"
	return f.err
}

func (f *fakeAlerts) GetPending() provider.AlertIterator {
	ch := make(chan *types.Alert)
	done := make(chan struct{})
	go func() {
		defer close(ch)
		for _, a := range f.alerts {
			ch <- a
		}
	}()
	return provider.NewAlertIterator(ch, done, f.err)
}
