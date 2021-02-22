// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

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
		numShards          int
		readURL            string
		writeURL           string
		concurrentPulls    int
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxSlabSizeBytes   int64
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		numShards:          4,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    false,
		concurrentPulls:    2,
		maxSlabSizeBytes:   500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:               conf.mint,
		Maxt:               conf.maxt,
		JobName:            conf.name,
		SlabSizeLimitBytes: conf.maxSlabSizeBytes,
		NumStores:          conf.concurrentPulls,
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		ProgressMetricURL:  conf.progressMetricURL,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quiet = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigSlabRead  = make(chan *plan.Slab)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, conf.concurrentPulls, sigSlabRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigSlabRead)
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
	if remoteReadStorage.Series() != remoteWriteStorage.Series() {
		t.Fatalf("read-storage series and write-storage series do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Series(), remoteWriteStorage.Series())
	}
	// Verify net samples count.
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples() {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage series: %d and write-storage series: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples())
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != 0 {
		t.Fatalf("progress-metric samples count do not match the number of slabs created: samples: %d and slabs created: %d", remoteWriteStorage.SamplesProgress(), write.Slabs())
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
		numShards          int
		readURL            string
		writeURL           string
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxSlabSizeBytes   int64
		concurrentPulls    int
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		numShards:          4,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    true,
		concurrentPulls:    2,
		maxSlabSizeBytes:   500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:               conf.mint,
		Maxt:               conf.maxt,
		JobName:            conf.name,
		SlabSizeLimitBytes: conf.maxSlabSizeBytes,
		NumStores:          conf.concurrentPulls,
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		ProgressMetricURL:  conf.progressMetricURL,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quiet = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigRead      = make(chan *plan.Slab)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, conf.concurrentPulls, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigSlabStop = make(chan struct{})
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

	time.Sleep(time.Millisecond * 100)
	read.SigSlabStop <- struct{}{}
	time.Sleep(time.Millisecond * 100)
	cancelFunc()
	previousWriteSlabs := write.Slabs()

	planner, proceed, err = plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quiet = true

	readErrChan = make(chan error)
	writeErrChan = make(chan error)
	sigRead = make(chan *plan.Slab)

	cont, cancelFunc = context.WithCancel(context.Background())
	read, err = reader.New(cont, conf.readURL, planner, conf.concurrentPulls, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err = writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigRead)
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
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Slabs()+previousWriteSlabs) {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage samples: %d and write-storage samples: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Slabs()+previousWriteSlabs))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Slabs()+previousWriteSlabs) {
		t.Fatalf("progress-metric samples count do not match the number of slabs created: progress metric samples: %d and write slabs: %d", remoteWriteStorage.SamplesProgress(), write.Slabs())
	}
}

func TestReaderWriterPlannerIntegrationWithHaltWithSlabSizeOverflow(t *testing.T) {
	var largeTimeSeries, tsMint, tsMaxt = generateVeryLargeTimeseries()
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t)
	defer remoteWriteStorage.Close()

	conf := struct {
		name               string
		mint               int64
		maxt               int64
		numShards          int
		readURL            string
		writeURL           string
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxSlabSizeBytes   int64
		concurrentPulls    int
	}{
		name:               "ci-migration",
		mint:               tsMint,
		maxt:               tsMaxt,
		numShards:          4,
		readURL:            readURL,
		writeURL:           writeURL,
		progressMetricURL:  progressURL,
		progressMetricName: "progress_metric",
		progressEnabled:    true,
		concurrentPulls:    1,
		maxSlabSizeBytes:   50 * 1024,
	}

	// Replicate main.
	// Replicate main.
	planConfig := &plan.Config{
		Mint:               conf.mint,
		Maxt:               conf.maxt,
		JobName:            conf.name,
		SlabSizeLimitBytes: conf.maxSlabSizeBytes,
		NumStores:          conf.concurrentPulls,
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		ProgressMetricURL:  conf.progressMetricURL,
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
	planner.Quiet = true

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigRead      = make(chan *plan.Slab)
	)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.New(cont, conf.readURL, planner, conf.concurrentPulls, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigSlabStop = make(chan struct{})
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run(readErrChan)
	write.Run(writeErrChan)

	time.Sleep(time.Millisecond * 100)
	read.SigSlabStop <- struct{}{}
	time.Sleep(time.Millisecond * 100)
	cancelFunc()
	previousWriteSlabs := write.Slabs()

	planner, proceed, err = plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}
	planner.Quiet = true

	readErrChan = make(chan error)
	writeErrChan = make(chan error)
	sigRead = make(chan *plan.Slab)

	cont, cancelFunc = context.WithCancel(context.Background())
	read, err = reader.New(cont, conf.readURL, planner, conf.concurrentPulls, sigRead)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err = writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigRead)
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
	if remoteReadStorage.Samples() != remoteWriteStorage.Samples()-int(write.Slabs()+previousWriteSlabs) {
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage samples: %d and write-storage samples: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples()-int(write.Slabs()+previousWriteSlabs))
	}
	// Verify the progress metric samples count.
	if remoteWriteStorage.SamplesProgress() != int(write.Slabs()+previousWriteSlabs) {
		t.Fatalf("progress-metric samples count do not match the number of slabs created: progress metric samples: %d and write slabs: %d", remoteWriteStorage.SamplesProgress(), write.Slabs())
	}
}
