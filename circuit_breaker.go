package main

import (
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/types"
)

const circuitBreakerCooldown = 30 * time.Second

type hostCircuitBreaker struct {
	open      atomic.Bool
	reason    atomic.Pointer[string]
	target    atomic.Pointer[string]
	setBy     atomic.Pointer[string]
	trippedAt atomic.Int64
}

// CircuitBreaker tracks whether outbound network calls should be short-circuited per host.
type CircuitBreaker struct {
	hosts sync.Map
}

// Trip opens the breaker for a specific host with the provided reason.
func (cb *CircuitBreaker) Trip(setBy types.NodeID, target string, cause error) {
	types.Assertf(cause != nil, "circuit breaker requires non-nil cause for target %s set_by %s", target, setBy)
	host := cb.targetHost(target)
	breaker := cb.getOrCreateHost(host)

	breaker.open.Store(true)
	breaker.storeString(&breaker.reason, fmt.Sprintf("network call to %s failed: %v", target, cause))
	breaker.storeString(&breaker.target, target)
	breaker.storeString(&breaker.setBy, string(setBy))
	breaker.trippedAt.Store(time.Now().UnixNano())
}

// Reset closes all host breakers and clears stored context.
func (cb *CircuitBreaker) Reset() {
	cb.hosts.Range(func(key, value any) bool {
		hostKey, ok := key.(string)
		types.Assertf(ok, "circuit breaker host key %v is not string", key)
		breaker, ok := value.(*hostCircuitBreaker)
		types.Assertf(ok, "circuit breaker value for host %s has unexpected type %T", hostKey, value)
		breaker.reset()
		cb.hosts.Delete(hostKey)
		return true
	})
}

// Snapshot returns the current breaker state for all hosts.
func (cb *CircuitBreaker) Snapshot(nodeID types.NodeID) types.CircuitBreakerSnapshot {
	snapshot := types.CircuitBreakerSnapshot{
		Hosts: make(map[string]types.CircuitBreakerHostSnapshot),
	}

	cb.hosts.Range(func(key, value any) bool {
		hostKey, ok := key.(string)
		types.Assertf(ok, "circuit breaker host key %v is not string", key)
		breaker, ok := value.(*hostCircuitBreaker)
		types.Assertf(ok, "circuit breaker value for host %s has unexpected type %T", hostKey, value)

		hostSnapshot := breaker.snapshot(nodeID)
		snapshot.Hosts[hostKey] = hostSnapshot
		if hostSnapshot.Open {
			snapshot.Open = true
			if snapshot.SetBy == "" {
				snapshot.SetBy = hostSnapshot.SetBy
			}
			if snapshot.Target == "" {
				snapshot.Target = hostSnapshot.Target
			}
			if snapshot.Reason == "" {
				snapshot.Reason = hostSnapshot.Reason
			}
		}
		return true
	})

	if snapshot.SetBy == "" {
		snapshot.SetBy = string(nodeID)
	}

	return snapshot
}

func (cb *CircuitBreaker) getOrCreateHost(host string) *hostCircuitBreaker {
	if existing := cb.loadHostBreaker(host); existing != nil {
		return existing
	}
	fresh := &hostCircuitBreaker{}
	actual, _ := cb.hosts.LoadOrStore(host, fresh)
	breaker, ok := actual.(*hostCircuitBreaker)
	types.Assertf(ok, "circuit breaker entry for host %s has unexpected type %T", host, actual)
	return breaker
}

func (cb *CircuitBreaker) loadHostBreaker(host string) *hostCircuitBreaker {
	value, ok := cb.hosts.Load(host)
	if !ok {
		return nil
	}
	breaker, ok := value.(*hostCircuitBreaker)
	types.Assertf(ok, "circuit breaker entry for host %s has unexpected type %T", host, value)
	return breaker
}

func (cb *CircuitBreaker) resetHost(host string) {
	breaker := cb.loadHostBreaker(host)
	if breaker == nil {
		return
	}
	breaker.reset()
	cb.hosts.Delete(host)
}

func (cb *CircuitBreaker) targetHost(target string) string {
	types.Assertf(target != "", "circuit breaker target must not be empty")
	parsed, err := url.Parse(target)
	if err == nil && parsed != nil && parsed.Host != "" {
		return parsed.Host
	}

	prefixed, prefixErr := url.Parse("http://" + target)
	if prefixErr == nil && prefixed != nil && prefixed.Host != "" {
		return prefixed.Host
	}

	types.Assertf(false, "unable to extract host from target %s: parse_err=%v prefix_err=%v", target, err, prefixErr)
	return target
}

func (cb *hostCircuitBreaker) storeString(ptr *atomic.Pointer[string], value string) {
	ptr.Store(&value)
}

func (cb *hostCircuitBreaker) loadString(ptr *atomic.Pointer[string]) string {
	value := ptr.Load()
	if value == nil {
		return ""
	}
	return *value
}

func (cb *hostCircuitBreaker) reset() {
	cb.open.Store(false)
	cb.reason.Store(nil)
	cb.target.Store(nil)
	cb.setBy.Store(nil)
	cb.trippedAt.Store(0)
}

func (cb *hostCircuitBreaker) snapshot(nodeID types.NodeID) types.CircuitBreakerHostSnapshot {
	snapshot := types.CircuitBreakerHostSnapshot{
		Open:      cb.open.Load(),
		Target:    cb.loadString(&cb.target),
		Reason:    cb.loadString(&cb.reason),
		SetBy:     cb.loadString(&cb.setBy),
		TrippedAt: cb.trippedAt.Load(),
	}
	if snapshot.SetBy == "" {
		snapshot.SetBy = string(nodeID)
	}
	return snapshot
}

// CheckCircuitBreaker returns an error if outbound network calls should be blocked.
func (c *Cluster) CheckCircuitBreaker(target string) error {
	host := c.circuitBreaker.targetHost(target)
	breaker := c.circuitBreaker.loadHostBreaker(host)
	if breaker == nil || !breaker.open.Load() {
		return nil
	}

	trippedAt := time.Unix(0, breaker.trippedAt.Load())
	if !trippedAt.IsZero() && time.Since(trippedAt) >= circuitBreakerCooldown {
		c.circuitBreaker.resetHost(host)
		return nil
	}

	hostSnapshot := breaker.snapshot(c.NodeId)
	elapsed := time.Since(trippedAt)
	return fmt.Errorf("circuit breaker open for host %s; refusing network call to %s; reason: %s; set_by: %s; target: %s; tripped_at: %s; elapsed: %s; cooldown: %s", host, target, hostSnapshot.Reason, hostSnapshot.SetBy, hostSnapshot.Target, trippedAt.Format(time.RFC3339Nano), elapsed, circuitBreakerCooldown)
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
