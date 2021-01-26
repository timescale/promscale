package integration_tests

import (
	"context"
	"os"
	"testing"
	"time"

	plan "github.com/timescale/promscale/pkg/migration-tool/planner"
	"github.com/timescale/promscale/pkg/migration-tool/reader"
	"github.com/timescale/promscale/pkg/migration-tool/utils"
	"github.com/timescale/promscale/pkg/migration-tool/writer"
)

var smallTimeSeries, stsMint, stsMaxt = generateSmallTimeseries()

func TestReaderWriterWithPromTSDBBlocksWithoutHaltAndProgressDisabled(t *testing.T) {
	tsdbPath := generatePromTSDBBlocks(t, smallTimeSeries)
	defer os.Remove(tsdbPath)
	remoteReadStorage, _ := createRemoteReadServer(t, smallTimeSeries)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t)
	defer remoteWriteStorage.Close()

	conf := struct {
		name               string
		mint               int64
		maxt               int64
		promTSDBpath       string
		readURL            string
		numShards          int
		numStores          int
		writeURL           string
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxBlockSizeBytes  int64
	}{
		name:               "ci-migration",
		mint:               stsMint,
		maxt:               stsMaxt,
		promTSDBpath:       tsdbPath,
		numShards:          4,
		numStores:          1,
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
		NumStores:           conf.numStores,
		NumShards:           conf.numShards,
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
	planner.Quiet = true

	sigBlockRead := make(chan *plan.WriterInput)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.NewPathRead(cont, conf.promTSDBpath, planner, sigBlockRead, false)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	readErrChan := read.Run()
	writeErrChan := write.Run()
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
		t.Fatalf("read-storage samples and write-storage samples do not match: read-storage samples: %d and write-storage samples: %d", remoteReadStorage.Samples(), remoteWriteStorage.Samples())
	}
}

func TestReaderWriterWithPromTSDBBlocksWithHalt(t *testing.T) {
	tsdbPath := generatePromTSDBBlocks(t, smallTimeSeries)
	defer os.Remove(tsdbPath)
	remoteReadStorage, _ := createRemoteReadServer(t, smallTimeSeries)
	defer remoteReadStorage.Close()
	remoteWriteStorage, writeURL, progressURL := createRemoteWriteServer(t)
	defer remoteWriteStorage.Close()

	conf := struct {
		name               string
		mint               int64
		maxt               int64
		promTSDBpath       string
		readURL            string
		writeURL           string
		numShards          int
		numStores          int
		progressMetricURL  string
		progressMetricName string
		progressEnabled    bool
		maxBlockSizeBytes  int64
	}{
		name:               "ci-migration",
		mint:               stsMint,
		maxt:               stsMaxt,
		promTSDBpath:       tsdbPath,
		writeURL:           writeURL,
		numShards:          4,
		numStores:          1,
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
		NumStores:           conf.numStores,
		NumShards:           conf.numShards,
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
	planner.Quiet = true

	sigBlockRead := make(chan *plan.WriterInput)
	cont, cancelFunc := context.WithCancel(context.Background())
	read, err := reader.NewPathRead(cont, conf.promTSDBpath, planner, sigBlockRead, true)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err := writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	read.Run()
	write.Run()

	time.Sleep(time.Millisecond * 10)
	read.SigStop()
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
	planner.Quiet = true

	sigBlockRead = make(chan *plan.WriterInput)

	cont, cancelFunc = context.WithCancel(context.Background())
	read, err = reader.NewPathRead(cont, conf.promTSDBpath, planner, sigBlockRead, false)
	if err != nil {
		t.Fatal("msg", "could not create reader", "error", err.Error())
	}
	write, err = writer.New(cont, conf.writeURL, conf.progressMetricName, conf.name, conf.numShards, conf.progressEnabled, sigBlockRead)
	if err != nil {
		t.Fatal("msg", "could not create writer", "error", err.Error())
	}

	readErrChan := read.Run()
	writeErrChan := write.Run()
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
