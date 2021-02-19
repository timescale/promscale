package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
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

func checkSeriesSet(ss storage.SeriesSet) error {
	if ws := ss.Warnings(); len(ws) > 0 {
		return ws[0]
	}
	if ss.Err() != nil {
		return ss.Err()
	}
	return nil
}

func getSeriesID(conf *BenchConfig, rawSeriesID uint64, seriesMultipliedIndex int) (seriesID uint64) {
	return (rawSeriesID * uint64(conf.SeriesMultiplier)) + uint64(seriesMultipliedIndex)
}

func RunFullSimulation(conf *BenchConfig, qmi *qmInfo, q storage.Querier, ws *walSimulator, runNumber int) (time.Time, int, error) {
	ss := q.Select(false, nil, labels.MustNewMatcher(labels.MatchRegexp, "", ".*"))

	sth, err := NewSeriesTimeHeap(conf, ss, qmi, runNumber == 0)
	if err != nil {
		return time.Time{}, 0, err
	}

	start := time.Now()
	startTs := int64(model.TimeFromUnixNano(start.UnixNano()))
	firstTs := sth[0].ts
	dataTimeStartTs := firstTs
	if conf.UseWallClockForDataTime {
		dataTimeStartTs = startTs
	}

	count := 0
	err = sth.Visit(func(rawTs int64, val float64, seriesID uint64) error {
		timestampDelta := int64(float64(rawTs-firstTs) / conf.RateMultiplier)
		dataTimestamp := dataTimeStartTs + timestampDelta
		wallTimestamp := startTs + timestampDelta

		if conf.RateControl {
			wait := time.Until(model.Time(wallTimestamp).Time())
			if wait > 0 {
				time.Sleep(wait)
			}
		}
		samplesSent := 0
		for seriesMultiplierIndex := 0; seriesMultiplierIndex < conf.SeriesMultiplier; seriesMultiplierIndex++ {
			samples := []record.RefSample{
				{
					Ref: getSeriesID(conf, seriesID, seriesMultiplierIndex),
					T:   dataTimestamp,
					V:   val,
				},
			}
			ws.Append(samples)
			samplesSent++
		}
		count += samplesSent
		qmi.highestTs.Set(float64(dataTimestamp / 1000))
		qmi.samplesIn.Incr(int64(samplesSent))
		return nil
	})
	if err == nil {
		err = checkSeriesSet(ss)
	}
	return start, count, err
}

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

	qmi, err := getQM(conf)
	if err != nil {
		return err
	}
	qmi.qm.Start()

	q, err := db.Querier(context.TODO(), conf.Mint, conf.Maxt)
	if err != nil {
		return err
	}
	defer q.Close()

	ws := NewWalSimulator(qmi)
	count := 0
	start := time.Time{}
	for i := 0; i < conf.RepeatedRuns; i++ {
		runStart, runCount, err := RunFullSimulation(conf, qmi, q, ws, i)
		if err != nil {
			return err
		}
		if start.IsZero() {
			start = runStart
		}
		count += runCount
		ewmaRateSent := qmi.samplesIn.Rate()
		took := time.Since(runStart)
		fmt.Println("single run took", took, "count", runCount, "ewma metric/s sent", ewmaRateSent, "metrics/s db", float64(runCount)/took.Seconds())
	}

	qmi.markStoppedSend()

	ewmaRateSent := qmi.samplesIn.Rate()
	ws.Stop()
	qmi.qm.Stop()
	took := time.Since(start)
	fmt.Println("took", took, "count", count, "ewma metric/s sent", ewmaRateSent, "metrics/s db", float64(count)/took.Seconds())
	return nil
}
