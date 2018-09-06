package util

import (
	"fmt"
	"github.com/timescale/prometheus-postgresql-adapter/log"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	electionInterval = time.Millisecond * 300
)

// Election defines an interface for adapter leader election.
// If you are running Prometheus in HA mode where each Prometheus instance sends data to corresponding adapter you probably
// want to allow writes into the database from only one adapter at the time. We need to elect a leader who can write to
// the database. If leader goes down, another leader is elected. Look at `lock.go` for an implementation based on PostgreSQL
// advisory locks. Should be easy to plug in different leader election implementations.
type Election interface {
	Id() string
	BecomeLeader() (bool, error)
	IsLeader() (bool, error)
	Resign() error
}

// Elector is `Election` wrapper that provides cross-cutting concerns(eg. logging) and some common features shared among all election implementations.
type Elector struct {
	election  Election
	scheduled bool
	ticker    *time.Ticker
}

func NewElector(election Election, scheduled bool) *Elector {
	var t *time.Ticker
	if scheduled {
		t = time.NewTicker(electionInterval)
	}
	elector := &Elector{election: election, ticker: t, scheduled: scheduled}
	if scheduled {
		go elector.scheduledElection()
	}
	return elector
}

func (e *Elector) Id() string {
	return e.election.Id()
}

func (e *Elector) BecomeLeader() (bool, error) {
	leader, err := e.election.BecomeLeader()
	if err != nil {
		log.Error("msg", "Error while trying to become a leader", "err", err)
	}
	if leader {
		log.Info("msg", "Instance became a leader", "groupId", e.Id())
	}
	return leader, err
}

func (e *Elector) IsLeader() (bool, error) {
	return e.election.IsLeader()
}

func (e *Elector) Resign() error {
	err := e.election.Resign()
	if err != nil {
		log.Error("err", "Failed to resign", "err", err)
	} else {
		log.Info("msg", "Instance is not a leader anymore")
	}
	return err
}

func (e *Elector) scheduledElection() {
	for {
		select {
		case <-e.ticker.C:
			e.Elect()
		}
	}
}

func (e *Elector) Elect() (bool, error) {
	leader, err := e.IsLeader()
	if err != nil {
		log.Error("msg", "Leader check failed", "err", err)
	} else if !leader {
		leader, err = e.BecomeLeader()
		if err != nil {
			log.Error("msg", "Failed while becoming a leader", "err", err)
		}
	}
	return leader, err
}

// RestElection is a REST interface allowing to plug in any external leader election mechanism.
// Remote service can use REST endpoints to manage leader election thus block or allow writes.
type RestElection struct {
	leader bool
	mutex  sync.RWMutex
}

func NewRestElection() *RestElection {
	r := &RestElection{}
	http.Handle("/admin/election/leader", r.handleLeader())
	return r
}

func (r *RestElection) handleLeader() http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
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
				err = r.Resign()
				if err != nil {
					log.Error("err", err)
					http.Error(response, err.Error(), http.StatusInternalServerError)
					return
				}
				fmt.Fprintf(response, "%v", true)
			case 1:
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

func (r *RestElection) Id() string {
	return ""
}

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

func (r *RestElection) IsLeader() (bool, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.leader, nil
}

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
