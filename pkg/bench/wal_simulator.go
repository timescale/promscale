package bench

import (
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/tsdb/record"
)

type walSimulator struct {
	ch chan []record.RefSample
	wg sync.WaitGroup
}

func NewWalSimulator(qmi *qmInfo) *walSimulator {
	sim := &walSimulator{
		ch: make(chan []record.RefSample, WALSimulatorChannelSize),
		wg: sync.WaitGroup{},
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
		if ok := qmi.qm.Append(samples); !ok {
			fmt.Println("qm append returned false")
		}
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
		fmt.Println("WARNING: WAL channel is full, which violates the simulation")
	}
}
