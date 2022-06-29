package bench

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/tsdb/record"
)

type walSimulator struct {
	conf       *BenchConfig
	ch         chan []record.RefSample
	wg         sync.WaitGroup
	lastReport time.Time
}

const reportEvery = time.Second * 10

func NewWalSimulator(qmi *qmInfo, conf *BenchConfig) *walSimulator {
	if conf.FakeSendDuration > 0 {
		fmt.Println("Warning: using fakeSendDuration -- not sending data to db")
	}
	sim := &walSimulator{
		conf: conf,
		ch:   make(chan []record.RefSample, WALSimulatorChannelSize),
		wg:   sync.WaitGroup{},
	}
	sim.wg.Add(1)
	go func() {
		sim.run(qmi)
		sim.wg.Done()
	}()
	return sim
}

func (ws *walSimulator) run(qmi *qmInfo) {
	for samples := range ws.ch {
		if ws.conf.FakeSendDuration == 0 {
			if ok := qmi.qm.Append(samples); !ok {
				fmt.Println("qm append returned false")
			}
		} else {
			time.Sleep(ws.conf.FakeSendDuration)
		}
		qmi.samplesWal.Incr(int64(len(samples)))
	}
}

func (ws *walSimulator) Stop() {
	close(ws.ch)
	ws.wg.Wait()
}

func (ws *walSimulator) Append(samples []record.RefSample) {
	select {
	case ws.ch <- samples:
	default:
		if time.Since(ws.lastReport) > reportEvery {
			fmt.Println("WARNING: WAL channel is full, which violates the simulation")
			ws.lastReport = time.Now()
		}
		ws.ch <- samples
	}
}
