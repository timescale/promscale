// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package end_to_end_tests

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
			CacheConfig:    cache.DefaultConfig,
			MaxConnections: -1,
		}

		pgClient, err := pgclient.NewClientWithPool(prometheus.NewRegistry(), conf, 1, db, db, nil, tenancy.NewNoopAuthorizer(), false)
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

		rulesCfg := rules.DefaultConfig
		rulesCfg.ResendDelay = time.Millisecond * 100
		rulesCfg.PrometheusConfigAddress = alertsConfigPath

		require.NoError(t, rules.Validate(&rulesCfg))
		require.True(t, rulesCfg.ContainsRules())

		rulesCtx, stopRuler := context.WithCancel(context.Background())
		defer stopRuler()

		manager, _, err := rules.NewManager(rulesCtx, prometheus.NewRegistry(), pgClient, &rulesCfg)
		require.NoError(t, err)

		require.NotNil(t, rulesCfg.PrometheusConfig)
		require.NoError(t, manager.ApplyConfig(rulesCfg.PrometheusConfig))

		// Start the dummy alertmanager.
		go func() {
			defer func() {
				stopRuler()
			}()
			// Start alerts receiver.
			err := waitForAlertWithName("Test")
			require.NoError(t, err) // If expected alert was received, no error will be returned.
		}()

		require.NoError(t, manager.Run(), "error running rules manager")
	})
}

// waitForAlertWithName looks for alert requests by reusing modules from alertmanager.
// Most of the implementation is coped from
// https://github.com/prometheus/alertmanager/blob/f958b8be84b870e363f7dafcbeb807b463269a75/cmd/alertmanager/main.go#L186
//
// The below code aims to be a dummy alertmanager, that can accept an alert from Promscale.
func waitForAlertWithName(expected string) error {
	apiAM, wg, handler, err := setupAM(expected) // This function contains the code from alertmanager.
	if err != nil {
		return fmt.Errorf("error setting up dummy alert server: %w", err)
	}

	router := route.New()
	mux := apiAM.Register(router, "") // URL same as that given in alerting config.

	server := http.Server{Addr: "localhost:9093", Handler: mux}
	wg.Add(1)
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			panic(err)
		}
	}()
	wg.Wait()
	server.Close()

	return handler.err
}

// The aim of this function is to abstract out the code of setting up alertmanager that is copied from alertmanager repo
// from the actual test.
func setupAM(expected string) (*api.API, *sync.WaitGroup, *fakeAlerts, error) {
	f, err := os.CreateTemp(os.TempDir(), "*.alerts.test")
	if err != nil {
		return nil, nil, nil, err
	}

	silences, err := silence.New(silence.Options{
		SnapshotFile: f.Name(),
		Retention:    time.Hour * 120,
		Logger:       log.GetLogger(),
		Metrics:      prometheus.DefaultRegisterer,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	var disp *dispatch.Dispatcher
	groupFn := func(routeFilter func(*dispatch.Route) bool, alertFilter func(*types.Alert, time.Time) bool) (dispatch.AlertGroups, map[model.Fingerprint][]string) {
		return disp.Groups(routeFilter, alertFilter)
	}

	alertsReceiverWG := new(sync.WaitGroup)
	alertsHandler := newFakeAlerts([]*types.Alert{}, false, alertsReceiverWG, expected)
	apiAM, err := api.New(api.Options{
		Alerts:     alertsHandler,
		Silences:   silences,
		StatusFunc: types.NewMarker(prometheus.NewRegistry()).Status,
		Logger:     log.GetLogger(),
		Registry:   prometheus.NewRegistry(),
		GroupFunc:  groupFn,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	conf, err := amConfig.LoadFile(amConfigPath)
	if err != nil {
		return nil, nil, nil, err
	}
	apiAM.Update(conf, func(model.LabelSet) {})
	return apiAM, alertsReceiverWG, alertsHandler, nil
}

// copied from
// https://github.com/prometheus/alertmanager/blob/f958b8be84b870e363f7dafcbeb807b463269a75/api/v1/api_test.go#L38
//
// fakeAlerts is a struct implementing the provider.Alerts interface for tests.
type fakeAlerts struct {
	fps               map[model.Fingerprint]int
	alerts            []*types.Alert
	expectedAlertName string
	wg                *sync.WaitGroup
	err               error
}

func newFakeAlerts(alerts []*types.Alert, withErr bool, wg *sync.WaitGroup, expectedAlertName string) *fakeAlerts {
	fps := make(map[model.Fingerprint]int)
	for i, a := range alerts {
		fps[a.Fingerprint()] = i
	}
	f := &fakeAlerts{
		alerts:            alerts,
		fps:               fps,
		expectedAlertName: expectedAlertName,
		wg:                wg,
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
	// Check if the first alert name matches with the expected alert name that is declared in Promscale rules config .
	if alerts[0].Name() == f.expectedAlertName { // Declared in /testdata/rules/alerts.yaml
		log.Info("msg", "alert received matches with expected alert name")
	} else {
		f.err = fmt.Errorf("received alert does not match with the expected alert name")
		log.Error("msg", f.err.Error())
	}
	f.wg.Done()
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
