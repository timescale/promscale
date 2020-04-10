// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package util

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/timescale/timescale-prometheus/pkg/log"
)

// Election defines an interface for adapter leader election.
// If you are running Prometheus in HA mode where each Prometheus instance sends data to corresponding adapter you probably
// want to allow writes into the database from only one adapter at the time. We need to elect a leader who can write to
// the database. If leader goes down, another leader is elected. Look at `lock.go` for an implementation based on PostgreSQL
// advisory locks. Should be easy to plug in different leader election implementations.
type Election interface {
	ID() string
	BecomeLeader() (bool, error)
	IsLeader() (bool, error)
	Resign() error
}

// Elector is `Election` wrapper that provides cross-cutting concerns(eg. logging) and some common features shared among all election implementations.
type Elector struct {
	election Election
}

// NewElector is a constructor for the Elector
func NewElector(election Election) *Elector {
	elector := &Elector{election: election}
	return elector
}

// ID returns the elector ID
func (e *Elector) ID() string {
	return e.election.ID()
}

// BecomeLeader attempts to make the node the leader
func (e *Elector) BecomeLeader() (bool, error) {
	leader, err := e.election.BecomeLeader()
	if err != nil {
		log.Error("msg", "Error while trying to become a leader", "err", err)
	}
	if leader {
		log.Info("msg", "Instance became a leader", "groupID", e.ID())
	}
	return leader, err
}

// IsLeader checks whether the node is the leader
func (e *Elector) IsLeader() (bool, error) {
	return e.election.IsLeader()
}

// Resign gives up leadership
func (e *Elector) Resign() error {
	err := e.election.Resign()
	if err != nil {
		log.Error("err", "Failed to resign", "err", err)
	} else {
		log.Info("msg", "Instance is no longer a leader")
	}
	return err
}

// ScheduledElector triggers election on scheduled interval. Currently used in combination with PgAdvisoryLock
type ScheduledElector struct {
	Elector
	ticker                  *time.Ticker
	pausedScheduledElection bool
}

// NewScheduledElector is the constructor
func NewScheduledElector(election Election, electionInterval time.Duration) *ScheduledElector {
	scheduledElector := &ScheduledElector{Elector: Elector{election}, ticker: time.NewTicker(electionInterval)}
	go scheduledElector.scheduledElection()
	return scheduledElector
}

func (se *ScheduledElector) pauseScheduledElection() {
	se.pausedScheduledElection = true
}

func (se *ScheduledElector) resumeScheduledElection() {
	se.pausedScheduledElection = false
}

func (se *ScheduledElector) isScheduledElectionPaused() bool {
	return se.pausedScheduledElection
}

// PrometheusLivenessCheck checks if the last request seen from prometheus and if it's older than a
// timeout, and if so, give up leadership.
func (se *ScheduledElector) PrometheusLivenessCheck(lastRequestUnixNano int64, timeout time.Duration) {
	elapsed := time.Since(time.Unix(0, lastRequestUnixNano))
	leader, err := se.IsLeader()
	if err != nil {
		log.Error("msg", err.Error())
	}
	if leader {
		if elapsed > timeout {
			log.Warn("msg", "Prometheus timeout exceeded", "timeout", timeout)
			se.pauseScheduledElection()
			log.Warn("msg", "Scheduled election is paused. Instance is removed from election pool.")
			err := se.Resign()
			if err != nil {
				log.Error("msg", err.Error())
			}
		}
	} else {
		if se.isScheduledElectionPaused() && elapsed < timeout {
			log.Info("msg", "Prometheus seems alive. Resuming scheduled election.")
			se.resumeScheduledElection()
		}
	}
}

func (se *ScheduledElector) scheduledElection() {
	for range se.ticker.C {
		if !se.pausedScheduledElection {
			se.elect()
		} else {
			log.Debug("msg", "Scheduled election is paused. Instance can't become a leader until scheduled election is resumed (Prometheus comes up again)")
		}
	}
}

func (se *ScheduledElector) elect() bool {
	leader, err := se.IsLeader()
	if err != nil {
		log.Error("msg", "Leader check failed", "err", err)
	} else if !leader {
		leader, err = se.BecomeLeader()
		if err != nil {
			log.Error("msg", "Failed while becoming a leader", "err", err)
		}
	}
	return leader
}

// RestElection is a REST interface allowing to plug in any external leader election mechanism.
// Remote service can use REST endpoints to manage leader election thus block or allow writes.
// Using RestElection over PgAdvisoryLock is encouraged as it is more robust and gives more control over
// the election process, however it does require additional engineering effort.
type RestElection struct {
	leader bool
	mutex  sync.RWMutex
}

// NewRestElection returns a new constructor
func NewRestElection() *RestElection {
	r := &RestElection{}
	http.Handle("/admin/election/leader", r.handleLeader())
	return r
}

func (r *RestElection) handleLeader() http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			// leader check
			leader, err := r.IsLeader()
			if err != nil {
				log.Error("msg", "Failed on leader check", "err", err)
				http.Error(response, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(response, "%v", leader)
		case http.MethodPut:
			body, err := ioutil.ReadAll(request.Body)
			if err != nil {
				log.Error("msg", "Error reading request body", "err", err)
				http.Error(response, "Can't read body", http.StatusBadRequest)
				return
			}
			flag, err := strconv.Atoi(string(body))
			if err != nil {
				log.Error("msg", "Error parsing to int", "body", string(body), "err", err)
				http.Error(response, "1 or 0 expected in request body", http.StatusBadRequest)
				return
			}
			switch flag {
			case 0:
				// resign
				err = r.Resign()
				if err != nil {
					log.Error("err", err)
					http.Error(response, err.Error(), http.StatusInternalServerError)
					return
				}
				fmt.Fprintf(response, "%v", true)
			case 1:
				// become a leader
				leader, err := r.BecomeLeader()
				if err != nil {
					log.Error("msg", "Failed to become a leader", "err", err)
					http.Error(response, err.Error(), http.StatusInternalServerError)
					return
				}
				fmt.Fprintf(response, "%v", leader)
			default:
				log.Error("msg", "Wrong number in request body", "body", string(body), "err", err)
				http.Error(response, "1 or 0 expected in request body", http.StatusBadRequest)
				return
			}
		default:
			log.Error("msg", "Request method not supported")
			http.Error(response, "Request method not supported", http.StatusBadRequest)
		}
	}
}

// ID returns an always-blank id
func (r *RestElection) ID() string {
	return ""
}

// BecomeLeader causes the election to try to grab leadership
func (r *RestElection) BecomeLeader() (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.leader {
		log.Warn("msg", "Instance is already a leader")
		return r.leader, nil
	}
	r.leader = true
	return r.leader, nil
}

// IsLeader returns whether or not you are the leader
func (r *RestElection) IsLeader() (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.leader, nil
}

// Resign gives up leadership
func (r *RestElection) Resign() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.leader {
		log.Warn("msg", "Can't resign when not a leader")
		return nil
	}
	r.leader = false
	return nil
}
