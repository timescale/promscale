package integration_tests

import (
	"context"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
	"testing"
)

var largeTimeSeries, tsMint, tsMaxt = generateLargeTimeseries()

func TestReaderWriterPlannerIntegrationWithoutHalts(t *testing.T) {
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t)
	defer remoteWriteStorage.Close()

	conf := struct {
		name               string
		mint               int64
		maxt               int64
		readURL            string
		writeURL           string
		writerReadURL      string
		progressMetricName string
		progressEnabled    bool
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		readURL:            readURL,
		writeURL:           writeURL,
		writerReadURL:      progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    false,
	}

	// Replicate main.
	planner, proceed, err := plan.CreatePlan(conf.mint, conf.maxt, conf.progressMetricName, conf.name, conf.writerReadURL, conf.progressEnabled)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}

	var (
		sigBlockWrite = make(chan struct{})
		sigBlockRead  = make(chan *plan.Block)
		readErrChan   = make(chan error)
		writeErrChan  = make(chan error)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, sigBlockRead, sigBlockWrite)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead, sigBlockWrite)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

	select {
	case err, ok := <-readErrChan:
		if ok {
			t.Fatal("msg", "running reader", "error", err.Error())
		}
		cancelFunc()
	case err = <-writeErrChan:
		cancelFunc()
		t.Fatal("msg", "running writer", "error", err.Error())
	}

	// Cross-verify the migration stats.
	// Verify series count.
	if remoteReadStorage.Series() != remoteWriteStorage.Series()-1 {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series()-1)
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Blocks()) {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Blocks()))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Blocks()) {
		t.Fatalf("progress-metric samples count do not match the number of blocks created")
	}
}
