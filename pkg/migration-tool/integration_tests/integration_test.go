package integration_tests

import (
	"sync"
	"testing"

	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

var largeTimeSeries, tsMint, tsMaxt = generateLargeTimeseries()

func TestReaderWriterPlannerIntegrationWithoutHaults(t *testing.T) {
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL := createRemoteWriteServer(t)
	defer remoteWriteStorage.Close()

	conf := struct {
		name                 string
		mint                 int64
		maxt                 int64
		readURL              string
		writeURL             string
		writerReadURL        string
		progressMetricName   string
		ignoreProgressMetric bool
	}{
		name:                 "ci-migration",
		mint:                 tsMint,
		maxt:                 tsMaxt,
		readURL:              readURL,
		writeURL:             writeURL,
		writerReadURL:        "",
		progressMetricName:   "progress_metric",
		ignoreProgressMetric: true,
	}

	// Replicate main.
	planner, proceed, err := plan.CreatePlan(conf.mint, conf.maxt, conf.progressMetricName, conf.writerReadURL, conf.ignoreProgressMetric)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}

	var (
		wg            sync.WaitGroup
		sigBlockRead  = make(chan *plan.Block)
		sigBlockWrite = make(chan struct{})
	)
	read, err := reader.New(conf.readURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(conf.writeURL, conf.progressMetricName, conf.name, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	wg.Add(2)
	// nolint:staticcheck
	go func() {
		defer wg.Done()
		if err = read.Run(); err != nil {
			t.Fatal("msg", "running reader", "error", err.Error())
		}
	}()
	// nolint:staticcheck
	go func() {
		defer wg.Done()
		if err = write.Run(); err != nil {
			t.Fatal("msg", "running writer", "error", err.Error())
		}
	}()
	wg.Wait()

	// Cross-verify the migration stats.
	// Verify series count.
	if remoteReadStorage.Series() != remoteWriteStorage.Series()-1 {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series()-1)
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Blocks()) {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Blocks()))
	}
}
