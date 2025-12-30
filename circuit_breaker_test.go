package main

import (
	"errors"
	"testing"

	"github.com/donomii/clusterF/types"
)

func TestCircuitBreakerIsPerHost(t *testing.T) {
	cluster := Cluster{NodeId: types.NodeID("node-1")}
	targetA := "http://host-a.test:8080/internal/files/foo"
	targetB := "http://host-b.test:8080/internal/files/bar"

	cluster.TripCircuitBreaker(targetA, errors.New("network unreachable"))

	if err := cluster.CheckCircuitBreaker(targetA); err == nil {
		t.Fatalf("expected host %s to be open after trip", targetA)
	}

	if err := cluster.CheckCircuitBreaker(targetB); err != nil {
		t.Fatalf("expected host %s to remain closed, got %v", targetB, err)
	}

	snapshot := cluster.CircuitBreakerStatus()
	if !snapshot.Open {
		t.Fatalf("expected snapshot to report open breaker")
	}

	hostSnapshot, ok := snapshot.Hosts["host-a.test:8080"]
	if !ok {
		t.Fatalf("expected host host-a.test:8080 in snapshot hosts")
	}
	if !hostSnapshot.Open {
		t.Fatalf("expected host-a.test:8080 breaker to be open")
	}
	if hostSnapshot.Target != targetA {
		t.Fatalf("expected target %s, got %s", targetA, hostSnapshot.Target)
	}

	if _, exists := snapshot.Hosts["host-b.test:8080"]; exists {
		t.Fatalf("did not expect host-b.test:8080 to be tracked when never tripped")
	}
}
