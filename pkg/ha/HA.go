package ha

import (
	"fmt"
	"sync"
	"time"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
)

type haWriter struct {
	DBClient        pgxconn.PgxConn
}

type writer interface {
	readHALocks(minT, maxT time.Time, clusterName string) (cluster, leader string, leaseStart, leaseUntil time.Time)
	changeLeader(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time)
}

func newHAWriter(dbClient *pgxconn.PgxConn) *haWriter {
	return &haWriter{
		DBClient: *dbClient,
	}
}

func (h *haWriter) readHALocks(minT, maxT time.Time, clusterName string) (cluster, leader string, leaseStart, leaseUntil time.Time) {
	return "", "", time.Time{}, time.Time{}
}

func (h *haWriter) changeLeader(minT, maxT time.Time, clusterName string) (leader string, leaseStart, leaseUntil time.Time) {
	return "", time.Time{}, time.Time{}
}

type State struct {
	cluster         string
	leader          string
	leaseStart      time.Time
	leaseUntil      time.Time
	maxTimeSeen     time.Time // max data time seen by any instance
	maxTimeInstance string    // the instance name that’s seen the maxtime
	mu              sync.Mutex
	writer          writer
}

func NewHAState(dbClient *pgxconn.PgxConn) *State {
	return &State{
		writer: newHAWriter(dbClient),
	}
}

// check whether the samples are from prom leader & in expected time range.
func (h *State) checkInsert(minT, maxT time.Time, clusterName, replicaName string) bool {
	latestCluster, latestLeader, leaseStart, leaseUntil := h.writer.readHALocks(minT, maxT, clusterName)
	if replicaName == latestLeader {
		h.updateHAState(latestCluster, latestLeader, replicaName, maxT, leaseStart, leaseUntil)
		return true
	}

	// check if leaseUntil is behind the maxTimeSeen
	// if yes try changing the leader
	if leaseUntil.Before(maxT)  {
		latestLeader, leaseStart, leaseUntil = h.writer.changeLeader(minT, maxT, clusterName)
		h.updateHAState(clusterName, latestLeader, replicaName, maxT, leaseStart, leaseUntil)
		return true
	}

	return false
}

func (h *State) updateHAState(clusterName, leaderName, replicaName string, maxT, leaseStart, leaseUntil time.Time) {
	h.mu.Lock()
	h.maxTimeSeen = maxT
	h.cluster = clusterName
	h.leader = leaderName
	h.maxTimeInstance = replicaName
	h.leaseStart = leaseStart
	h.leaseUntil = leaseUntil
	h.mu.Unlock()
}

// Parse data into a set of samplesInfo infos per-metric.
// returns: map[metric name][]SamplesInfo, total rows to insert
// NOTE: req will be added to our WriteRequest pool in this function, it must
//       not be used afterwards.
// When Prometheus & Promscale are running HA mode the below parseData is used
// to validate leader replica samples & ha_locks in TimescaleDB.
func (h *State) ParseData(tts []prompb.TimeSeries, req *prompb.WriteRequest) (map[string][]model.SamplesInfo, int, error) {
	dataSamples := make(map[string][]model.SamplesInfo)
	rows := 0

	var minT, maxT int64

	var replicaName, clusterName string
	if len(tts) > 0 {
		s, _, err := model.LabelProtosToLabels(tts[0].Labels)
		if err != nil {
			return nil, rows, err
		}
		replicaName = s.GetReplicaName()
		clusterName = s.GetClusterName()
	}



	if replicaName == "" || clusterName == "" {
		err := fmt.Errorf("ha mode is enabled and __replica__ or __cluster__ is empty")
		return nil, rows, err
	}

	for i := range tts {
		t := &tts[i]
		if len(t.Samples) == 0 {
			continue
		}

		if t.Samples[0].Timestamp < minT || minT == 0 {
			minT = t.Samples[0].Timestamp
		}

		if t.Samples[len(t.Samples)-1].Timestamp > maxT {
			maxT = t.Samples[len(t.Samples)-1].Timestamp
		}

		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		seriesLabels, metricName, err := model.LabelProtosToLabels(t.Labels)
		if err != nil {
			return nil, rows, err
		}
		if metricName == "" {
			return nil, rows, errors.ErrNoMetricName
		}
		sample := model.SamplesInfo{
			Labels:   seriesLabels,
			SeriesID: -1, // sentinel marking the seriesId as unset
			Samples:  t.Samples,
		}
		rows += len(t.Samples)

		dataSamples[metricName] = append(dataSamples[metricName], sample)
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		t.Samples = nil
	}

	minTUnix := time.Unix(minT, 0)
	maxTUnix := time.Unix(maxT, 0)
	if !h.checkInsert(minTUnix, maxTUnix, clusterName, replicaName) {
		log.Debug("the samples aren't from the leader prom instance. skipping the insert")
		return nil, rows, nil
	}

	return dataSamples, rows, nil
}
