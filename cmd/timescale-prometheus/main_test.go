package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/timescale/timescale-prometheus/pkg/api"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/pgclient"
	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/util"
	"github.com/timescale/timescale-prometheus/pkg/version"
)

type mockElection struct {
	isLeader bool
	err      error
}

func (m *mockElection) ID() string {
	return "ID"
}

func (m *mockElection) BecomeLeader() (bool, error) {
	return true, nil
}

func (m *mockElection) IsLeader() (bool, error) {
	return m.isLeader, m.err
}

func (m *mockElection) Resign() error {
	return nil
}

type mockGauge struct {
	value float64
}

func (m *mockGauge) Desc() *prometheus.Desc {
	panic("not implemented")
}

func (m *mockGauge) Write(_ *dto.Metric) error {
	panic("not implemented")
}

func (m *mockGauge) Describe(_ chan<- *prometheus.Desc) {
	panic("not implemented")
}

func (m *mockGauge) Collect(_ chan<- prometheus.Metric) {
	panic("not implemented")
}

func (m *mockGauge) Set(v float64) {
	m.value = v
}

func (m *mockGauge) Inc() {
	panic("not implemented")
}

func (m *mockGauge) Dec() {
	panic("not implemented")
}

func (m *mockGauge) Add(_ float64) {
	panic("not implemented")
}

func (m *mockGauge) Sub(_ float64) {
	panic("not implemented")
}

func (m *mockGauge) SetToCurrentTime() {
	panic("not implemented")
}

func TestMain(m *testing.M) {
	flag.Parse()
	err := log.Init("debug")

	if err != nil {
		fmt.Println("Error initializing logger", err)
		os.Exit(1)
	}
	code := m.Run()
	os.Exit(code)
}

func TestInitElector(t *testing.T) {
	// TODO: refactor the function to be fully testable without using a DB.
	testCases := []struct {
		name         string
		cfg          *config
		shouldError  bool
		electionType reflect.Type
	}{
		{
			name: "Cannot create REST election with a group lock ID",
			cfg: &config{
				haGroupLockID: 1,
				restElection:  true,
			},
			shouldError: true,
		},
		{
			name: "Create REST elector",
			cfg: &config{
				haGroupLockID: 0,
				restElection:  true,
			},
			electionType: reflect.TypeOf(&util.RestElection{}),
		},
		{
			name: "Cannot create scheduled elector, no group lock ID and not rest election",
			cfg: &config{
				haGroupLockID: 0,
			},
		},
		{
			name: "Prometheus timeout not set for PG advisory lock",
			cfg: &config{
				haGroupLockID:     1,
				prometheusTimeout: -1,
			},
			shouldError: true,
		},
		{
			name: "Can't get advisory lock, couldn't connect to DB",
			cfg: &config{
				haGroupLockID:     1,
				prometheusTimeout: 0,
			},
			shouldError: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			metrics := api.InitMetrics()
			elector, err := initElector(c.cfg, metrics)

			switch {
			case err != nil && !c.shouldError:
				t.Errorf("Unexpected error, got %s", err.Error())
			case err == nil && c.shouldError:
				t.Errorf("Expected error, got nil")
			}

			if c.electionType != nil {
				if elector == nil {
					t.Fatalf("Expected to create elector, got nil")
				}

				v := reflect.ValueOf(elector).Elem().Field(0).Elem()

				if v.Type() != c.electionType {
					t.Errorf("Wrong type of elector created: got %v wanted %v", v.Type(), c.electionType)
				}
			}
		})
	}
}

func TestMigrate(t *testing.T) {
	testCases := []struct {
		name        string
		cfg         *pgclient.Config
		isLeader    bool
		electionErr error
		shouldError bool
	}{
		{
			name:        "elector error",
			electionErr: fmt.Errorf("some error"),
			shouldError: true,
		},
		{
			name: "not a leader",
		},
		{
			name:        "is leader",
			isLeader:    true,
			cfg:         &pgclient.Config{},
			shouldError: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			elector = util.NewElector(
				&mockElection{
					isLeader: c.isLeader,
					err:      c.electionErr,
				},
			)
			metrics := api.InitMetrics()
			mockGauge := &mockGauge{}
			metrics.LeaderGauge = mockGauge

			err := migrate(c.cfg, pgmodel.VersionInfo{Version: version.Version}, metrics)

			switch {
			case err != nil && !c.shouldError:
				t.Errorf("Unexpected error returned:\ngot\n%s\nwanted nil\n", err)
			case err == nil && c.shouldError:
				t.Errorf("Expected error to be returned: got nil")
			}

			switch {
			case c.isLeader && mockGauge.value != 1:
				t.Errorf("Leader gauge metric not set correctly: got %f when is leader", mockGauge.value)
			case !c.isLeader && mockGauge.value != 0:
				t.Errorf("Leader gauge metric not set correctly: got %f when is not leader", mockGauge.value)
			}
		})
	}
}
