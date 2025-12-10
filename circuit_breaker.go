package main

import (
	"fmt"
	"sync/atomic"

	"github.com/donomii/clusterF/types"
)

// CircuitBreaker tracks whether outbound network calls should be short-circuited.
type CircuitBreaker struct {
	open   atomic.Bool
	reason atomic.Value
	target atomic.Value
	setBy  atomic.Value
}

// Trip opens the breaker with the provided reason.
func (cb *CircuitBreaker) Trip(setBy types.NodeID, target string, cause error) {
	if cause == nil {
		return
	}

	cb.open.Store(true)
	cb.reason.Store(fmt.Sprintf("network call to %s failed: %v", target, cause))
	cb.target.Store(target)
	cb.setBy.Store(string(setBy))
}

// Reset closes the breaker.
func (cb *CircuitBreaker) Reset() {
	cb.open.Store(false)
}

// IsOpen reports whether the breaker is currently open.
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.open.Load()
}

// Reason returns the reason recorded when the breaker was tripped.
func (cb *CircuitBreaker) Reason() string {
	value := cb.reason.Load()
	if value == nil {
		return ""
	}

	reason, ok := value.(string)
	if !ok {
		return ""
	}
	return reason
}

// Snapshot returns the current breaker state.
func (cb *CircuitBreaker) Snapshot(nodeID types.NodeID) types.CircuitBreakerSnapshot {
	snapshot := types.CircuitBreakerSnapshot{
		Open: cb.open.Load(),
	}

	setByValue := cb.setBy.Load()
	if setByValue != nil {
		setBy, ok := setByValue.(string)
		if ok {
			snapshot.SetBy = setBy
		}
	}
	if snapshot.SetBy == "" {
		snapshot.SetBy = string(nodeID)
	}

	targetValue := cb.target.Load()
	if targetValue != nil {
		target, ok := targetValue.(string)
		if ok {
			snapshot.Target = target
		}
	}

	reasonValue := cb.reason.Load()
	if reasonValue != nil {
		reason, ok := reasonValue.(string)
		if ok {
			snapshot.Reason = reason
		}
	}

	return snapshot
}

// CheckCircuitBreaker returns an error if outbound network calls should be blocked.
func (c *Cluster) CheckCircuitBreaker(target string) error {
	if !c.circuitBreaker.IsOpen() {
		return nil
	}

	reason := c.circuitBreaker.Reason()
	if reason == "" {
		reason = "unspecified failure"
	}
	return fmt.Errorf("circuit breaker open; refusing network call to %s; reason: %s; set_by: %s", target, reason, c.circuitBreaker.Snapshot(c.NodeId).SetBy)
}

// TripCircuitBreaker records a network failure and opens the breaker.
func (c *Cluster) TripCircuitBreaker(target string, cause error) {
	c.circuitBreaker.Trip(c.NodeId, target, cause)
}

func (c *Cluster) ConnectedStatus() bool {
	c.refreshConnectedFromDiscovery()
	return c.connected.Load()
}

func (c *Cluster) CircuitBreakerStatus() types.CircuitBreakerSnapshot {
	return c.circuitBreaker.Snapshot(c.NodeId)
}
