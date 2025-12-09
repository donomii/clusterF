// metrics.go - Performance metrics collection and publishing
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/threadmanager"
)

// MetricsCollector collects and publishes performance metrics
type MetricsCollector struct {
	publishFunc       func(key string, payload []byte)
	threadMgr         *threadmanager.ThreadManager
	nodeID            string
	publishChan       chan struct{}
	counters          syncmap.SyncMap[string, *int64]
	timers            syncmap.SyncMap[string, *TimerMetric]
	mu                sync.RWMutex
	lastPublish       time.Time
	localCancel       context.CancelFunc
	backgroundWg      sync.WaitGroup
	breakerProvider   func() CircuitBreakerSnapshot
	connectedProvider func() ConnectionSnapshot
}

// TimerMetric tracks timing statistics
type TimerMetric struct {
	count      int64
	totalNanos int64
	minNanos   int64
	maxNanos   int64
	mu         sync.RWMutex
}

// MetricsSnapshot represents a point-in-time view of metrics
type MetricsSnapshot struct {
	NodeID         string                 `json:"node_id"`
	Timestamp      int64                  `json:"timestamp"`
	Counters       map[string]int64       `json:"counters"`
	Timers         map[string]TimerStats  `json:"timers"`
	Runtime        RuntimeStats           `json:"runtime"`
	CircuitBreaker CircuitBreakerSnapshot `json:"circuit_breaker"`
	Connection     ConnectionSnapshot     `json:"connection"`
}

// CircuitBreakerSnapshot captures the current breaker state for a node.
type CircuitBreakerSnapshot struct {
	Open   bool   `json:"open"`
	SetBy  string `json:"set_by"`
	Target string `json:"target"`
	Reason string `json:"reason"`
}

// ConnectionSnapshot captures whether the node currently sees peers.
type ConnectionSnapshot struct {
	Connected bool `json:"connected"`
}

// TimerStats contains aggregated timer statistics
type TimerStats struct {
	Count   int64   `json:"count"`
	TotalMs float64 `json:"total_ms"`
	AvgMs   float64 `json:"avg_ms"`
	MinMs   float64 `json:"min_ms"`
	MaxMs   float64 `json:"max_ms"`
}

// RuntimeStats contains Go runtime statistics
type RuntimeStats struct {
	Goroutines int    `json:"goroutines"`
	MemAllocMB uint64 `json:"mem_alloc_mb"`
	MemSysMB   uint64 `json:"mem_sys_mb"`
	NumGC      uint32 `json:"num_gc"`
	NextGCMB   uint64 `json:"next_gc_mb"`
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(publishFunc func(key string, payload []byte), threadMgr *threadmanager.ThreadManager, nodeID string) *MetricsCollector {
	if publishFunc == nil {
		panic("metrics publisher function cannot be nil")
	}

	mc := &MetricsCollector{
		publishFunc: publishFunc,
		threadMgr:   threadMgr,
		nodeID:      nodeID,
		publishChan: make(chan struct{}, 1),
		lastPublish: time.Now(),
	}

	startPublisher := func(ctx context.Context) {
		mc.publisherLoop(ctx)
	}
	startScheduler := func(ctx context.Context) {
		mc.schedulerLoop(ctx)
	}

	if err := threadMgr.StartThreadWithRestart("metrics-publisher", startPublisher, true, nil); err != nil {
		panic(fmt.Sprintf("failed to start metrics publisher thread: %v", err))
	}
	if err := threadMgr.StartThreadWithRestart("metrics-scheduler", startScheduler, true, nil); err != nil {
		panic(fmt.Sprintf("failed to start metrics scheduler thread: %v", err))
	}

	return mc
}

// IncrementCounter atomically increments a counter metric
func (mc *MetricsCollector) IncrementCounter(name string) {
	counter, _ := mc.counters.LoadOrStore(name, new(int64))
	atomic.AddInt64(counter, 1)
}

// AddCounter atomically adds a value to a counter metric
func (mc *MetricsCollector) AddCounter(name string, value int64) {
	counter, _ := mc.counters.LoadOrStore(name, new(int64))
	atomic.AddInt64(counter, value)
}

// RecordTiming records a timing measurement
func (mc *MetricsCollector) RecordTiming(name string, duration time.Duration) {
	timer, _ := mc.timers.LoadOrStore(name, &TimerMetric{
		minNanos: duration.Nanoseconds(),
		maxNanos: duration.Nanoseconds(),
	})

	timer.mu.Lock()
	defer timer.mu.Unlock()

	nanos := duration.Nanoseconds()
	timer.count++
	timer.totalNanos += nanos

	if timer.count == 1 || nanos < timer.minNanos {
		timer.minNanos = nanos
	}
	if timer.count == 1 || nanos > timer.maxNanos {
		timer.maxNanos = nanos
	}
}

// StartTimer returns a function that records timing when called
func (mc *MetricsCollector) StartTimer(name string) func() {
	start := time.Now()
	return func() {
		mc.RecordTiming(name, time.Since(start))
	}
}

// SetCircuitBreakerProvider registers a callback to include breaker state in snapshots.
func (mc *MetricsCollector) SetCircuitBreakerProvider(provider func() CircuitBreakerSnapshot) {
	mc.breakerProvider = provider
}

// SetConnectedProvider registers a callback to include connection state in snapshots.
func (mc *MetricsCollector) SetConnectedProvider(provider func() ConnectionSnapshot) {
	mc.connectedProvider = provider
}

// RequestPublish signals that metrics should be published
func (mc *MetricsCollector) RequestPublish() {
	select {
	case mc.publishChan <- struct{}{}:
	default:
		// Channel full, publish already pending
	}
}

// publisherLoop handles background metric publishing
func (mc *MetricsCollector) publisherLoop(ctx context.Context) {
	for {
		select {
		case <-mc.publishChan:
			mc.publishMetrics()
		case <-ctx.Done():
			return
		}
	}
}

func (mc *MetricsCollector) schedulerLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mc.RequestPublish()
		case <-ctx.Done():
			return
		}
	}
}

// publishMetrics publishes current metrics to the CRDT
func (mc *MetricsCollector) publishMetrics() {
	snapshot := mc.captureSnapshot()

	key := fmt.Sprintf("metrics/%s", mc.nodeID)

	data, err := json.Marshal(snapshot)
	if err != nil {
		return // Silently ignore marshal errors
	}

	mc.publishFunc(key, data)
	mc.lastPublish = time.Now()
}

// captureSnapshot creates a point-in-time metrics snapshot
func (mc *MetricsCollector) captureSnapshot() MetricsSnapshot {
	snapshot := MetricsSnapshot{
		NodeID:    mc.nodeID,
		Timestamp: time.Now().Unix(),
		Counters:  make(map[string]int64),
		Timers:    make(map[string]TimerStats),
		Runtime:   mc.captureRuntimeStats(),
	}

	// Capture counters
	mc.counters.Range(func(name string, counter *int64) bool {
		snapshot.Counters[name] = atomic.LoadInt64(counter)
		return true
	})

	// Capture timers
	mc.timers.Range(func(name string, timer *TimerMetric) bool {
		timer.mu.RLock()
		stats := TimerStats{
			Count:   timer.count,
			TotalMs: float64(timer.totalNanos) / 1000000.0,
			MinMs:   float64(timer.minNanos) / 1000000.0,
			MaxMs:   float64(timer.maxNanos) / 1000000.0,
		}
		if timer.count > 0 {
			stats.AvgMs = stats.TotalMs / float64(timer.count)
		}
		timer.mu.RUnlock()

		snapshot.Timers[name] = stats
		return true
	})

	if mc.breakerProvider != nil {
		snapshot.CircuitBreaker = mc.breakerProvider()
	}
	if mc.connectedProvider != nil {
		snapshot.Connection = mc.connectedProvider()
	}

	return snapshot
}

// captureRuntimeStats captures Go runtime statistics
func (mc *MetricsCollector) captureRuntimeStats() RuntimeStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return RuntimeStats{
		Goroutines: runtime.NumGoroutine(),
		MemAllocMB: memStats.Alloc / 1024 / 1024,
		MemSysMB:   memStats.Sys / 1024 / 1024,
		NumGC:      memStats.NumGC,
		NextGCMB:   memStats.NextGC / 1024 / 1024,
	}
}

// Close shuts down the metrics collector
func (mc *MetricsCollector) Close() {
	if mc.localCancel != nil {
		mc.localCancel()
		mc.backgroundWg.Wait()
	}
}

// Global metrics collector instance
var globalCollector *MetricsCollector
var collectorMutex sync.RWMutex

// SetGlobalCollector sets the global metrics collector instance
func SetGlobalCollector(collector *MetricsCollector) {
	collectorMutex.Lock()
	defer collectorMutex.Unlock()
	globalCollector = collector
}

// GetGlobalCollector returns the global metrics collector instance
func GetGlobalCollector() *MetricsCollector {
	collectorMutex.RLock()
	defer collectorMutex.RUnlock()
	return globalCollector
}

// IncrementGlobalCounter atomically increments a counter metric on the global collector
func IncrementGlobalCounter(name string) {
	collectorMutex.RLock()
	collector := globalCollector
	collectorMutex.RUnlock()

	if collector != nil {
		collector.IncrementCounter(name)
	}
}

// AddGlobalCounter atomically adds value to a counter metric on the global collector
func AddGlobalCounter(name string, value int64) {
	collectorMutex.RLock()
	collector := globalCollector
	collectorMutex.RUnlock()

	if collector != nil {
		collector.AddCounter(name, value)
	}
}

// RecordGlobalTiming records a timing measurement on the global collector
func RecordGlobalTiming(name string, duration time.Duration) {
	collectorMutex.RLock()
	collector := globalCollector
	collectorMutex.RUnlock()

	if collector != nil {
		collector.RecordTiming(name, duration)
	}
}

// StartGlobalTimer returns a function that records timing when called on the global collector
func StartGlobalTimer(name string) func() {
	start := time.Now()
	return func() {
		RecordGlobalTiming(name, time.Since(start))
	}
}
