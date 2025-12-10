// metrics.go - Performance metrics collection and storage
package metrics

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
)

// MetricsCollector collects and writes performance metrics
type MetricsCollector struct {
	nodeID   string
	counters syncmap.SyncMap[string, *int64]
	timers   syncmap.SyncMap[string, *TimerMetric]
	mu       sync.RWMutex
	app      *types.App
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
	NodeID         string                       `json:"node_id"`
	Timestamp      int64                        `json:"timestamp"`
	Counters       map[string]int64             `json:"counters"`
	Timers         map[string]TimerStats        `json:"timers"`
	Runtime        RuntimeStats                 `json:"runtime"`
	CircuitBreaker types.CircuitBreakerSnapshot `json:"circuit_breaker"`
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
func NewMetricsCollector(app *types.App) *MetricsCollector {
	mc := &MetricsCollector{
		app:    app,
		nodeID: string(app.NodeID),
	}
	collectorMutex.Lock()
	globalCollector = mc
	collectorMutex.Unlock()
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

// StoreMetrics writes current metrics to the CRDT
func (mc *MetricsCollector) StoreMetrics() {
	snapshot := mc.captureSnapshot()

	key := fmt.Sprintf("metrics/%s", mc.nodeID)

	data, err := json.Marshal(snapshot)
	types.Assertf(err == nil, "failed to marshal metrics snapshot: %v", err)

	mc.app.Frogpond.SetDataPoint(key, data)
}

// captureSnapshot creates a point-in-time metrics snapshot
func (mc *MetricsCollector) captureSnapshot() MetricsSnapshot {
	snapshot := MetricsSnapshot{
		NodeID:         mc.nodeID,
		Timestamp:      time.Now().Unix(),
		Counters:       make(map[string]int64),
		Timers:         make(map[string]TimerStats),
		Runtime:        mc.captureRuntimeStats(),
		CircuitBreaker: mc.app.Cluster.CircuitBreakerStatus(),
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

}

// Global metrics collector instance
var globalCollector *MetricsCollector
var collectorMutex sync.RWMutex

// GetGlobalCollector returns the global metrics collector instance
func GetGlobalCollector() *MetricsCollector {
	collectorMutex.RLock()
	defer collectorMutex.RUnlock()
	types.Assert(globalCollector != nil, "no global collector")
	return globalCollector
}

// IncrementGlobalCounter atomically increments a counter metric on the global collector
func IncrementGlobalCounter(name string) {
	GetGlobalCollector().IncrementCounter(name)
}

// AddGlobalCounter atomically adds value to a counter metric on the global collector
func AddGlobalCounter(name string, value int64) {
	GetGlobalCollector().AddCounter(name, value)
}

// RecordGlobalTiming records a timing measurement on the global collector
func RecordGlobalTiming(name string, duration time.Duration) {
	GetGlobalCollector().RecordTiming(name, duration)
}

// StartGlobalTimer records the start time for a timer on the global collector.
func StartGlobalTimer(name string) time.Time {
	return time.Now()
}

// FinishGlobalTimer records elapsed time for a timer started with StartGlobalTimer.
func FinishGlobalTimer(name string, started time.Time) {
	RecordGlobalTiming(name, time.Since(started))
}
