package util

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRestElection(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	re := NewRestElection()
	if leader, _ := re.IsLeader(); leader {
		t.Error("Initially there is no leader")
	}
	if leader, _ := re.BecomeLeader(); !leader {
		t.Error("Failed to elect")
	}
	if leader, _ := re.IsLeader(); !leader {
		t.Error("Failed to elect")
	}
	re.Resign()
	if leader, _ := re.IsLeader(); leader {
		t.Error("Failed to resign")
	}
}

func TestRESTApi(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	re := NewRestElection()
	becomeLeaderReq, err := http.NewRequest("PUT", "/admin/leader", bytes.NewReader([]byte("1")))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, becomeLeaderReq)

	if recorder.Code != 200 {
		t.Error("Expected HTTP 200 Status Code")
	}
	if recorder.Body.String() != "true" {
		t.Error("Failed to become a leader")
	}

	leaderCheckReq, err := http.NewRequest("GET", "/admin/leader", nil)
	recorder = httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, leaderCheckReq)
	if recorder.Body.String() != "true" {
		t.Error("Instance should be leader")
	}

	resignReq, err := http.NewRequest("PUT", "/admin/leader", bytes.NewReader([]byte("0")))
	recorder = httptest.NewRecorder()
	re.handleLeader().ServeHTTP(recorder, resignReq)

	if recorder.Code != 200 {
		t.Error("Expected HTTP 200 Status Code")
	}
	if recorder.Body.String() != "true" {
		t.Error("Failed to resign")
	}
}
