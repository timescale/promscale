package integration_tests

import (
	"context"
	"testing"
	"time"

	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
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
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxBlockSizeBytes  int64
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    false,
		maxBlockSizeBytes:  500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:                conf.mint,
		Maxt:                conf.maxt,
		JobName:             conf.name,
		BlockSizeLimitBytes: conf.maxBlockSizeBytes,
		ProgressEnabled:     conf.progressEnabled,
		ProgressMetricName:  conf.progressMetricName,
		ProgressMetricURL:   conf.progressMetricURL,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quite = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigBlockRead = make(chan *plan.Block)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)
loop:
	for {
		select {
		case err = <-readErrChan:
			if err != nil {
				cancelFunc()
				t.Fatal("msg", "running reader", "error", err.Error())
			}
		case err, ok := <-writeErrChan:
			cancelFunc() // As in any ideal case, the reader will always exit normally first.
			if ok {
				t.Fatal("msg", "running writer", "error", err.Error())
			}
			break loop
		}
	}

	// Cross-verify the migration stats.
	// Verify series count.
	if remoteReadStorage.Series() != remoteWriteStorage.Series()-1 {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series()-1)
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Blocks()) {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Blocks()))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Blocks()) {
		t.Fatalf("progress-metric samples count do not match the number of blocks created")
	}
}

func TestReaderWriterPlannerIntegrationWithHalt(t *testing.T) {
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
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxBlockSizeBytes  int64
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    true,
		maxBlockSizeBytes:  500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:                conf.mint,
		Maxt:                conf.maxt,
		JobName:             conf.name,
		BlockSizeLimitBytes: conf.maxBlockSizeBytes,
		ProgressEnabled:     conf.progressEnabled,
		ProgressMetricName:  conf.progressMetricName,
		ProgressMetricURL:   conf.progressMetricURL,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quite = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigBlockRead = make(chan *plan.Block)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigForceStop = make(chan struct{})
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

	time.Sleep(time.Millisecond * 100)
	read.SigForceStop <- struct{}{}
	time.Sleep(time.Millisecond * 100)
	cancelFunc()
	previousWriteBlocks := write.Blocks()

	planner, proceed, err = plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quite = true

	readErrChan = make(chan error)
	writeErrChan = make(chan error)
	sigBlockRead = make(chan *plan.Block)

	cont, cancelFunc = context.WithCancel(context.Background())
	read, err = reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err = writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)
loop:
	for {
		select {
		case err = <-readErrChan:
			if err != nil {
				cancelFunc()
				t.Fatal("msg", "running reader", "error", err.Error())
			}
		case err, ok := <-writeErrChan:
			cancelFunc() // As in any ideal case, the reader will always exit normally first.
			if ok {
				t.Fatal("msg", "running writer", "error", err.Error())
			}
			break loop
		}
	}

	// Cross-verify the migration stats.
	// Verify series count.
	if remoteReadStorage.Series() != remoteWriteStorage.Series()-1 {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series()-1)
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Blocks()+previousWriteBlocks) {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage samples: %d and write-storage samples: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Blocks()+previousWriteBlocks))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Blocks()+previousWriteBlocks) {
		t.Fatalf("progress-metric samples count do not match the number of blocks created: progress metric samples: %d and write blocks: %d", remoteWriteStorage.SamplesProgress(), write.Blocks())
	}
}

func TestReaderWriterPlannerIntegrationWithHaltWithBlockSizeOverflow(t *testing.T) {
	var largeTimeSeries, tsMint, tsMaxt = generateVeryLargeTimeseries()
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
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxBlockSizeBytes  int64
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    true,
		maxBlockSizeBytes:  50 * 1024,
	}

	// Replicate main.
	// Replicate main.
	planConfig := &plan.Config{
		Mint:                conf.mint,
		Maxt:                conf.maxt,
		JobName:             conf.name,
		BlockSizeLimitBytes: conf.maxBlockSizeBytes,
		ProgressEnabled:     conf.progressEnabled,
		ProgressMetricName:  conf.progressMetricName,
		ProgressMetricURL:   conf.progressMetricURL,
	}
	planner, proceed, err := plan.Init(planConfig)
	planner.TestCheckFunc = func() {
		if planner.LastMemoryFootprint()*100/utils.Megabyte > 4 {
			t.Fatal("memory footprint greater than expected")
		}
	}
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quite = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigBlockRead = make(chan *plan.Block)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigForceStop = make(chan struct{})
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

	time.Sleep(time.Millisecond * 100)
	read.SigForceStop <- struct{}{}
	time.Sleep(time.Millisecond * 100)
	cancelFunc()
	previousWriteBlocks := write.Blocks()

	planner, proceed, err = plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quite = true

	readErrChan = make(chan error)
	writeErrChan = make(chan error)
	sigBlockRead = make(chan *plan.Block)

	cont, cancelFunc = context.WithCancel(context.Background())
	read, err = reader.New(cont, conf.readURL, planner, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err = writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)
loop:
	for {
		select {
		case err = <-readErrChan:
			if err != nil {
				cancelFunc()
				t.Fatal("msg", "running reader", "error", err.Error())
			}
		case err, ok := <-writeErrChan:
			cancelFunc() // As in any ideal case, the reader will always exit normally first.
			if ok {
				t.Fatal("msg", "running writer", "error", err.Error())
			}
			break loop
		}
	}

	// Cross-verify the migration stats.
	// Verify series count.
	if remoteReadStorage.Series() != remoteWriteStorage.Series()-1 {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series()-1)
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Blocks()+previousWriteBlocks) {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage samples: %d and write-storage samples: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Blocks()+previousWriteBlocks))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Blocks()+previousWriteBlocks) {
		t.Fatalf("progress-metric samples count do not match the number of blocks created: progress metric samples: %d and write blocks: %d", remoteWriteStorage.SamplesProgress(), write.Blocks())
	}
}
