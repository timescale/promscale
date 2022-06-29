package bench

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/record"

	"github.com/prometheus/prometheus/tsdb"
)

const (
	/* copied from queue_manager.go */
	ewmaWeight          = 0.2
	shardUpdateDuration = 10 * time.Second

	//from cli flag storage.remote.flush-deadline in main.go
	//How long to wait flushing sample on shutdown or config reload.
	defaultFlushDeadline = 1 * time.Minute

	WALSimulatorChannelSize = 100000000
)

var rateControlSleeps int32 = 0

func RunFullSimulation(conf *BenchConfig, qmi *qmInfo, block *tsdb.Block, ws *walSimulator, runNumber int) (time.Time, int, error) {
	dm := NewDataModifier(conf)
	sth, closers, err := NewSeriesTimeHeap(dm, block, qmi, seriesIndex)
	if err != nil {
		return time.Time{}, 0, err
	}
	defer func() {
		for _, c := range closers {
			err := c.Close()
			if err != nil {
				panic(err)
			}
		}
	}()

	start := time.Now()
	count := 0
	err = sth.Visit(dm, func(recs []record.RefSample, wallTs int64) error {
		if conf.RateControl {
			wait := time.Until(model.Time(wallTs).Time())
			if wait > 0 {
				atomic.AddInt32(&rateControlSleeps, 1)
				time.Sleep(wait)
			}
		}
		ws.Append(recs)
		count++
		prevHighest := qmi.highestTs.Get()
		if prevHighest != 0 && prevHighest > float64(recs[0].T/1000) {
			fmt.Printf("Time going backwards! prevHighest=%v this=%v count=%v, wallTs=%v, T=%v, wall=%v, now=%v\n config=%#v\n",
				prevHighest, float64(recs[0].T/1000), count, wallTs, recs[0].T, model.Time(wallTs).Time(), time.Now(), conf)
			sth.Debug()
			fmt.Printf("\n")
		}
		qmi.highestTs.Set(float64(recs[0].T / 1000))
		qmi.samplesIn.Incr(int64(1))
		return nil
	})
	return start, count, err
}

var seriesIndex = 0

func Run(conf *BenchConfig) (err error) {
	db, err := tsdb.OpenDBReadOnly(conf.TSDBPath, nil)
	if err != nil {
		return err
	}
	defer func() {
		err2 := db.Close()
		if err == nil {
			err = err2
		}
	}()

	blocks, err := db.Blocks()
	if err != nil {
		return err
	}
	fmt.Println("Number of Blocks", len(blocks))

	qmi, err := getQM(conf)
	if err != nil {
		return err
	}
	qmi.qm.Start()

	ws := NewWalSimulator(qmi)
	totalCount := 0
	start := time.Time{}
	for i := 0; i < conf.RepeatedRuns; i++ {
		var runDuration time.Duration
		runCount := 0
		for blockIdx := 0; blockIdx < len(blocks); blockIdx++ {
			err := func() error {
				block := blocks[blockIdx].(*tsdb.Block)
				fmt.Println("Starting processing block", block.Meta().MinTime, block.Meta().MaxTime)

				blockStart, blockCount, err := RunFullSimulation(conf, qmi, block, ws, i)
				if err != nil {
					return err
				}
				if start.IsZero() {
					start = blockStart
				}
				totalCount += blockCount
				runCount += blockCount
				ewmaRateSent := qmi.samplesIn.Rate()
				blockDuration := time.Since(blockStart)
				runDuration += blockDuration

				fmt.Println("single block took", blockDuration, "count", blockCount, "ewma metric/s sent", ewmaRateSent, "metrics/s db", float64(blockCount)/blockDuration.Seconds(), "maximum lag", qmi.timeLagRealMax)

				qmi.qm.SeriesReset(seriesIndex)
				seriesIndex++
				return nil
			}()
			if err != nil {
				return err
			}
			//runtime.GC()
		}
		fmt.Println("single run took", runDuration, "count", runCount, "metrics/s db", float64(runCount)/runDuration.Seconds(), "maximum lag", qmi.timeLagRealMax)

	}

	qmi.markStoppedSend()

	ewmaRateSent := qmi.samplesIn.Rate()
	ws.Stop()
	qmi.qm.Stop()
	took := time.Since(start)
	fmt.Println("took", took, "count", totalCount, "ewma metric/s sent", ewmaRateSent, "metrics/s db", float64(totalCount)/took.Seconds(), "maximum lag", qmi.timeLagRealMax)
	return nil
}
