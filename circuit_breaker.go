package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/types"
)

const circuitBreakerCooldown = 30 * time.Second

// CircuitBreaker tracks whether outbound network calls should be short-circuited.
type CircuitBreaker struct {
	open      atomic.Bool
	reason    atomic.Pointer[string]
	target    atomic.Pointer[string]
	setBy     atomic.Pointer[string]
	trippedAt atomic.Int64
}

// Trip opens the breaker with the provided reason.
func (cb *CircuitBreaker) Trip(setBy types.NodeID, target string, cause error) {
	types.Assertf(cause != nil, "circuit breaker requires non-nil cause for target %s set_by %s", target, setBy)

	cb.open.Store(true)
	cb.storeString(&cb.reason, fmt.Sprintf("network call to %s failed: %v", target, cause))
	cb.storeString(&cb.target, target)
	cb.storeString(&cb.setBy, string(setBy))
	cb.trippedAt.Store(time.Now().UnixNano())
}

// Reset closes the breaker and clears stored context.
func (cb *CircuitBreaker) Reset() {
	cb.open.Store(false)
	cb.reason.Store(nil)
	cb.target.Store(nil)
	cb.setBy.Store(nil)
	cb.trippedAt.Store(0)
}

// IsOpen reports whether the breaker is currently open.
func (cb *CircuitBreaker) IsOpen() bool {
	return cb.open.Load()
}

// Reason returns the reason recorded when the breaker was tripped.
func (cb *CircuitBreaker) Reason() string {
	return cb.loadString(&cb.reason)
}

// Snapshot returns the current breaker state.
func (cb *CircuitBreaker) Snapshot(nodeID types.NodeID) types.CircuitBreakerSnapshot {
	snapshot := types.CircuitBreakerSnapshot{
		Open: cb.open.Load(),
	}

	setByValue := cb.loadString(&cb.setBy)
	if setByValue != "" {
		snapshot.SetBy = setByValue
	}
	if snapshot.SetBy == "" {
		snapshot.SetBy = string(nodeID)
	}

	snapshot.Target = cb.loadString(&cb.target)
	snapshot.Reason = cb.loadString(&cb.reason)

	return snapshot
}

func (cb *CircuitBreaker) storeString(ptr *atomic.Pointer[string], value string) {
	ptr.Store(&value)
}

func (cb *CircuitBreaker) loadString(ptr *atomic.Pointer[string]) string {
	value := ptr.Load()
	if value == nil {
		return ""
	}
	return *value
}

// CheckCircuitBreaker returns an error if outbound network calls should be blocked.
func (c *Cluster) CheckCircuitBreaker(target string) error {
	if !c.circuitBreaker.IsOpen() {
		return nil
	}

	tripped := c.circuitBreaker.trippedAt.Load()
	trippedAt := time.Unix(0, tripped)
	if !trippedAt.IsZero() && time.Since(trippedAt) >= circuitBreakerCooldown {
		c.circuitBreaker.Reset()
		return nil
	}

	reason := c.circuitBreaker.Reason()
	elapsed := time.Since(trippedAt)
	return fmt.Errorf("circuit breaker open; refusing network call to %s; reason: %s; set_by: %s; tripped_at: %s; elapsed: %s; cooldown: %s", target, reason, c.circuitBreaker.Snapshot(c.NodeId).SetBy, trippedAt.Format(time.RFC3339Nano), elapsed, circuitBreakerCooldown)
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
