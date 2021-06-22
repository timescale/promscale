// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/common/config"
	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

var (
	largeTimeSeries, tsMint, tsMaxt = generateLargeTimeseries()
	defaultRuntime                  = utils.ClientConfig{
		Timeout:   time.Minute * 5,
		OnTimeout: utils.Retry,
		OnErr:     utils.Retry,
		Delay:     time.Millisecond * 10,
		MaxRetry:  6,
	}
)

func getConfig(url string) utils.ClientConfig {
	r := defaultRuntime
	r.URL = url
	return r
}

func TestReaderWriterPlannerIntegrationWithoutHalts(t *testing.T) {
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries, true)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t, true, true)
	defer remoteWriteStorage.Close()

	conf := struct {
		name               string
		mint               int64
		maxt               int64
		numShards          int
		readURL            string
		writeURL           string
		concurrentPull     int
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		laIncrement        time.Duration
		maxReadDuration    time.Duration
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
		laIncrement:        time.Minute * 7,
		maxReadDuration:    time.Hour * 3,
		concurrentPull:     2,
		maxSlabSizeBytes:   500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:                 conf.mint,
		Maxt:                 conf.maxt,
		JobName:              conf.name,
		SlabSizeLimitBytes:   conf.maxSlabSizeBytes,
		NumStores:            conf.concurrentPull,
		ProgressEnabled:      conf.progressEnabled,
		ProgressMetricName:   conf.progressMetricName,
		ProgressClientConfig: getConfig(conf.progressMetricURL),
		LaIncrement:          conf.laIncrement,
		MaxReadDuration:      conf.maxReadDuration,
	}
	planner, proceed, err := plan.Init(planConfig)
	if err != nil {
		t.Fatal("msg", "could not create plan", "error", err.Error())
	}
	if !proceed {
		t.Fatal("could not proceed")
	}

	var (
		readErrChan  = make(chan error)
		writeErrChan = make(chan error)
		sigSlabRead  = make(chan *plan.Slab)
	)
	cont, cancelFunc := context.WithCancel(context.Background())

	readerConfig := reader.Config{
		Context:         cont,
		ClientConfig:    getConfig(conf.readURL),
		Plan:            planner,
		HTTPConfig:      config.HTTPClientConfig{},
		ConcurrentPulls: conf.concurrentPull,
		SigSlabRead:     sigSlabRead,
	}
	read, err := reader.New(readerConfig)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}

	writerConfig := writer.Config{
		Context:            cont,
		ClientConfig:       getConfig(conf.writeURL),
		HTTPConfig:         config.HTTPClientConfig{},
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		MigrationJobName:   conf.name,
		ConcurrentPush:     conf.numShards,
		SigSlabRead:        sigSlabRead,
	}
	write, err := writer.New(writerConfig)
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
	if !remoteWriteStorage.AreReceivedSamplesOrdered() {
		t.Fatal("received samples not in order")
	}
}

func TestReaderWriterPlannerIntegrationWithHalt(t *testing.T) {
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries, true)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t, true, true)
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
		laIncrement        time.Duration
		maxReadDuration    time.Duration
		maxSlabSizeBytes   int64
		concurrentPull     int
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
		laIncrement:        time.Minute,
		maxReadDuration:    time.Hour,
		concurrentPull:     2,
		maxSlabSizeBytes:   500 * utils.Megabyte,
	}

	// Replicate main.
	planConfig := &plan.Config{
		Mint:                 conf.mint,
		Maxt:                 conf.maxt,
		JobName:              conf.name,
		SlabSizeLimitBytes:   conf.maxSlabSizeBytes,
		NumStores:            conf.concurrentPull,
		ProgressEnabled:      conf.progressEnabled,
		ProgressMetricName:   conf.progressMetricName,
		ProgressClientConfig: getConfig(conf.progressMetricURL),
		LaIncrement:          conf.laIncrement,
		MaxReadDuration:      conf.maxReadDuration,
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
	readerConfig := reader.Config{
		Context:         cont,
		ClientConfig:    getConfig(conf.readURL),
		Plan:            planner,
		HTTPConfig:      config.HTTPClientConfig{},
		ConcurrentPulls: conf.concurrentPull,
		SigSlabRead:     sigRead,
	}
	read, err := reader.New(readerConfig)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigSlabStop = make(chan struct{})

	writerConfig := writer.Config{
		Context:            cont,
		ClientConfig:       getConfig(conf.writeURL),
		HTTPConfig:         config.HTTPClientConfig{},
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		MigrationJobName:   conf.name,
		ConcurrentPush:     conf.numShards,
		SigSlabRead:        sigRead,
	}
	write, err := writer.New(writerConfig)
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

	readerConfig.Context = cont
	readerConfig.SigSlabRead = sigRead
	read, err = reader.New(readerConfig)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}

	writerConfig.Context = cont
	writerConfig.SigSlabRead = sigRead
	write, err = writer.New(writerConfig)
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
	// Verify orderedness of received samples.
	if !remoteWriteStorage.AreReceivedSamplesOrdered() {
		t.Fatal("received samples not in order")
	}
}

func TestReaderWriterPlannerIntegrationWithHaltWithSlabSizeOverflow(t *testing.T) {
	var largeTimeSeries, tsMint, tsMaxt = generateVeryLargeTimeseries()
	remoteReadStorage, readURL := createRemoteReadServer(t, largeTimeSeries, false)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t, true, false)
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
		laIncrement        time.Duration
		maxReadDuration    time.Duration
		maxSlabSizeBytes   int64
		concurrentPull     int
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
		laIncrement:        time.Minute * 3,
		maxReadDuration:    time.Hour,
		concurrentPull:     1,
		maxSlabSizeBytes:   50 * 1024,
	}

	// Replicate main.
	// Replicate main.
	planConfig := &plan.Config{
		Mint:                 conf.mint,
		Maxt:                 conf.maxt,
		JobName:              conf.name,
		SlabSizeLimitBytes:   conf.maxSlabSizeBytes,
		NumStores:            conf.concurrentPull,
		ProgressEnabled:      conf.progressEnabled,
		ProgressMetricName:   conf.progressMetricName,
		ProgressClientConfig: getConfig(conf.progressMetricURL),
		LaIncrement:          conf.laIncrement,
		MaxReadDuration:      conf.maxReadDuration,
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

	readerConfig := reader.Config{
		Context:         cont,
		ClientConfig:    getConfig(conf.readURL),
		Plan:            planner,
		HTTPConfig:      config.HTTPClientConfig{},
		ConcurrentPulls: conf.concurrentPull,
		SigSlabRead:     sigRead,
	}
	read, err := reader.New(readerConfig)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	read.SigSlabStop = make(chan struct{})

	writerConfig := writer.Config{
		Context:            cont,
		ClientConfig:       getConfig(conf.writeURL),
		HTTPConfig:         config.HTTPClientConfig{},
		ProgressEnabled:    conf.progressEnabled,
		ProgressMetricName: conf.progressMetricName,
		MigrationJobName:   conf.name,
		ConcurrentPush:     conf.numShards,
		SigSlabRead:        sigRead,
	}
	write, err := writer.New(writerConfig)
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

	readerConfig.Context = cont
	readerConfig.SigSlabRead = sigRead
	read, err = reader.New(readerConfig)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}

	writerConfig.Context = cont
	writerConfig.SigSlabRead = sigRead
	write, err = writer.New(writerConfig)
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
	// Verify orderedness of received samples.
	if !remoteWriteStorage.AreReceivedSamplesOrdered() {
		t.Fatal("received samples not in order")
	}
}
