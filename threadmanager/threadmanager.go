// threadmanager.go - Centralized goroutine lifecycle management
package threadmanager

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/donomii/clusterF/syncmap"
)

// ThreadInfo holds information about a managed goroutine
type ThreadInfo struct {
	Name          string
	StartTime     time.Time
	RestartCount  int
	Context       context.Context
	Cancel        context.CancelFunc
	Done          chan struct{}
	IsRunning     bool
	Function      func(ctx context.Context) // Store the function for restart
	Cleanup       func()                    // Optional cleanup function
	ShouldRestart bool                      // Whether to restart on unexpected exit
}

// ThreadManager manages all goroutines for a cluster node
type ThreadManager struct {
	nodeID          string
	logger          *log.Logger
	threads         *syncmap.SyncMap[string, *ThreadInfo]
	ctx             context.Context
	cancel          context.CancelFunc
	shutdownTimeout time.Duration
	shuttingDown    bool // Flag to prevent restarts during shutdown
	monitorCtx      context.Context
	monitorCancel   context.CancelFunc
	wg              sync.WaitGroup // For monitoring goroutine
	Debug           bool
}

func panicf(format string, v ...interface{}) {
	panic(fmt.Sprintf(format, v...))
}

// Debugf logs a debug message if debug is enabled
func (tm *ThreadManager) Debugf(format string, v ...interface{}) {
	if tm.Debug {
		tm.logger.Printf(format, v...)
	}
}

// NewThreadManager creates a new thread manager for a node
func NewThreadManager(nodeID string, logger *log.Logger) *ThreadManager {
	ctx, cancel := context.WithCancel(context.Background())
	monitorCtx, monitorCancel := context.WithCancel(context.Background())

	if logger == nil {
		logger = log.New(log.Writer(), fmt.Sprintf("[%s-TM] ", nodeID), log.LstdFlags|log.Lshortfile)
	}

	tm := &ThreadManager{
		nodeID:          nodeID,
		logger:          logger,
		threads:         syncmap.NewSyncMap[string, *ThreadInfo](),
		ctx:             ctx,
		cancel:          cancel,
		shutdownTimeout: 15 * time.Second, // Default 15s shutdown timeout
		shuttingDown:    false,
		monitorCtx:      monitorCtx,
		monitorCancel:   monitorCancel,
	}

	// Start the monitoring goroutine
	tm.wg.Add(1)
	go tm.monitorThreads()

	return tm
}

// runThread executes a thread with proper error handling and cleanup
func (tm *ThreadManager) runThread(threadInfo *ThreadInfo) {
	defer func() {
		// Handle panics in threads
		if r := recover(); r != nil {
			tm.logger.Printf("Thread '%s' panicked: %v\nStack: %s", threadInfo.Name, r, debug.Stack())
			tm.logger.Printf("Thread '%s' will be marked as not finished", threadInfo.Name)
		}

		// Run cleanup if provided
		if threadInfo.Cleanup != nil {
			threadInfo.Cleanup()
		}

		// Mark thread as finished
		if info, exists := tm.threads.Load(threadInfo.Name); exists {
			info.IsRunning = false
			tm.threads.Store(threadInfo.Name, info)
		}

		close(threadInfo.Done)
		tm.Debugf("Thread '%s' stopped (restart count: %d)", threadInfo.Name, threadInfo.RestartCount)
	}()

	// Run the actual thread function
	threadInfo.Function(threadInfo.Context)
}

// monitorThreads continuously monitors thread health and restarts failed threads
func (tm *ThreadManager) monitorThreads() {
	defer tm.wg.Done()

	ticker := time.NewTicker(1 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-tm.monitorCtx.Done():
			return
		case <-ticker.C:
			tm.checkAndRestartThreads()
		}
	}
}

// checkAndRestartThreads checks for dead threads and restarts them if needed
func (tm *ThreadManager) checkAndRestartThreads() {
	// Don't restart threads if we're shutting down
	if tm.shuttingDown {
		return
	}

	tm.threads.Range(func(name string, threadInfo *ThreadInfo) bool {
		// Skip if thread is still running
		if threadInfo.IsRunning {
			return true
		}

		// Skip if thread shouldn't be restarted
		if !threadInfo.ShouldRestart {
			return true
		}

		// Check if the thread's done channel is closed (indicating it exited)
		select {
		case <-threadInfo.Done:
			// Thread has exited, restart it
			tm.Debugf("Thread '%s' unexpectedly exited, restarting...", name)

			// Create new context and done channel
			newCtx, newCancel := context.WithCancel(tm.ctx)
			newDone := make(chan struct{})

			// Update thread info
			threadInfo.Context = newCtx
			threadInfo.Cancel = newCancel
			threadInfo.Done = newDone
			threadInfo.IsRunning = true
			threadInfo.StartTime = time.Now()
			threadInfo.RestartCount++

			// Store updated thread info
			tm.threads.Store(name, threadInfo)

			// Restart the thread
			go tm.runThread(threadInfo)

		default:
			// Thread is still running or hasn't signaled completion yet
		}
		return true
	})
}

// SetShutdownTimeout configures how long to wait for threads to shut down
func (tm *ThreadManager) SetShutdownTimeout(timeout time.Duration) {
	tm.shutdownTimeout = timeout
}

// StartThread starts a managed goroutine with the given name and function
func (tm *ThreadManager) StartThread(name string, fn func(ctx context.Context)) error {
	return tm.StartThreadWithRestart(name, fn, true, nil)
}

// StartThreadOnce starts a managed goroutine that runs once and does not restart
func (tm *ThreadManager) StartThreadOnce(name string, fn func(ctx context.Context)) error {
	return tm.StartThreadWithRestart(name, fn, false, nil)
}

// StartThreadWithRestart starts a managed goroutine with restart control
func (tm *ThreadManager) StartThreadWithRestart(name string, fn func(ctx context.Context), shouldRestart bool, cleanup func()) error {
	// Check if thread with this name already exists and is running
	if existing, exists := tm.threads.Load(name); exists && existing.IsRunning {
		panicf("thread '%s' is already running", name)
	}

	// Create thread context that will be cancelled when the manager shuts down
	threadCtx, threadCancel := context.WithCancel(tm.ctx)
	done := make(chan struct{})

	threadInfo := &ThreadInfo{
		Name:          name,
		StartTime:     time.Now(),
		RestartCount:  0,
		Context:       threadCtx,
		Cancel:        threadCancel,
		Done:          done,
		IsRunning:     true,
		Function:      fn,
		Cleanup:       cleanup,
		ShouldRestart: shouldRestart,
	}

	// If this is a restart, increment the restart count
	if existing, exists := tm.threads.Load(name); exists {
		threadInfo.RestartCount = existing.RestartCount + 1
	}

	tm.threads.Store(name, threadInfo)
	tm.Debugf("Starting thread '%s' (restart count: %d)", name, threadInfo.RestartCount)

	// Start the goroutine
	go tm.runThread(threadInfo)

	return nil
}

// StartThreadWithCleanup starts a thread with a cleanup function that runs on shutdown
func (tm *ThreadManager) StartThreadWithCleanup(name string, fn func(ctx context.Context), cleanup func()) error {
	return tm.StartThread(name, func(ctx context.Context) {
		defer cleanup()
		fn(ctx)
	})
}

// StopThread stops a specific thread by name
func (tm *ThreadManager) StopThread(name string, timeout time.Duration) error {
	threadInfo, exists := tm.threads.Load(name)
	if !exists {
		return fmt.Errorf("thread '%s' not found", name)
	}

	if !threadInfo.IsRunning {
		return nil // Already stopped
	}

	tm.Debugf("Stopping thread '%s'", name)

	// Cancel the thread's context
	threadInfo.Cancel()

	// Wait for it to finish with timeout
	select {
	case <-threadInfo.Done:
		tm.Debugf("Thread '%s' stopped gracefully", name)
		return nil
	case <-time.After(timeout):
		tm.Debugf("Thread '%s' failed to stop within %v", name, timeout)
		return fmt.Errorf("thread '%s' failed to stop within %v", name, timeout)
	}
}

// GetThreadStatus returns the current status of all threads
func (tm *ThreadManager) GetThreadStatus() map[string]ThreadStatus {
	status := make(map[string]ThreadStatus)
	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		status[name] = ThreadStatus{
			Name:          info.Name,
			IsRunning:     info.IsRunning,
			StartTime:     info.StartTime,
			Uptime:        time.Since(info.StartTime),
			RestartCount:  info.RestartCount,
			ShouldRestart: info.ShouldRestart,
		}
		return true
	})
	return status
}

// ThreadStatus represents the status of a thread
type ThreadStatus struct {
	Name          string
	IsRunning     bool
	StartTime     time.Time
	Uptime        time.Duration
	RestartCount  int
	ShouldRestart bool
}

// ListRunningThreads returns names of all currently running threads
func (tm *ThreadManager) ListRunningThreads() []string {
	var running []string
	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		if info.IsRunning {
			running = append(running, name)
		}
		return true
	})
	return running
}

// IsThreadRunning checks if a specific thread is running
func (tm *ThreadManager) IsThreadRunning(name string) bool {
	if info, exists := tm.threads.Load(name); exists {
		return info.IsRunning
	}
	return false
}

// Shutdown gracefully shuts down all managed threads
func (tm *ThreadManager) Shutdown() []string {
	tm.Debugf("ThreadManager for node %s initiating shutdown", tm.nodeID)

	// Set shutdown flag to prevent restarts
	tm.shuttingDown = true

	// Stop the monitoring goroutine
	tm.monitorCancel()

	threadsToWaitFor := make(map[string]*ThreadInfo)
	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		if info.IsRunning {
			threadsToWaitFor[name] = info
		}
		return true
	})

	// Cancel the main context to signal all threads to stop
	tm.cancel()

	if len(threadsToWaitFor) == 0 {
		tm.Debugf("No running threads to shutdown")
		// Wait for monitor to finish
		tm.wg.Wait()
		return nil
	}

	tm.Debugf("Waiting for %d threads to shutdown: %v",
		len(threadsToWaitFor), getThreadNames(threadsToWaitFor))

	// Wait for all threads to finish with timeout
	shutdownComplete := make(chan struct{})
	go func() {
		defer close(shutdownComplete)
		for name, info := range threadsToWaitFor {
			select {
			case <-info.Done:
				tm.Debugf("Thread '%s' shutdown gracefully", name)
			case <-time.After(tm.shutdownTimeout):
				tm.Debugf("Thread '%s' shutdown timeout", name)
			}
		}
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-shutdownComplete:
		tm.Debugf("All threads shutdown completed")
	case <-time.After(tm.shutdownTimeout + 1*time.Second):
		tm.Debugf("Shutdown timeout exceeded")
	}

	// Wait for monitor to finish
	tm.wg.Wait()

	// Check which threads failed to stop
	var failedThreads []string
	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		if info.IsRunning {
			failedThreads = append(failedThreads, name)
			tm.Debugf("ThreadManager for node %s failed to shutdown, thread '%s' will not quit", tm.nodeID, name)
		}
		return true
	})

	if len(failedThreads) == 0 {
		tm.Debugf("ThreadManager for node %s shutdown successfully", tm.nodeID)
	} else {
		tm.logger.Printf("ThreadManager for node %s shutdown with %d failed threads: %v",
			tm.nodeID, len(failedThreads), failedThreads)
	}

	return failedThreads
}

// ForceShutdown forcefully terminates all threads (use with caution)
func (tm *ThreadManager) ForceShutdown() {
	tm.logger.Printf("FORCE SHUTDOWN: ThreadManager for node %s", tm.nodeID)

	// Cancel all thread contexts
	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		if info.IsRunning {
			tm.logger.Printf("Force stopping thread '%s'", name)
			info.Cancel()
			info.IsRunning = false
			tm.threads.Store(name, info)
		}
		return true
	})

	tm.cancel()
}

// GetContext returns the manager's context (for non-managed goroutines that want to respect shutdown)
func (tm *ThreadManager) GetContext() context.Context {
	return tm.ctx
}

// WaitForThread waits for a specific thread to finish
func (tm *ThreadManager) WaitForThread(name string, timeout time.Duration) error {
	threadInfo, exists := tm.threads.Load(name)
	if !exists {
		return fmt.Errorf("thread '%s' not found", name)
	}

	if !threadInfo.IsRunning {
		return nil // Already stopped
	}

	select {
	case <-threadInfo.Done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for thread '%s'", name)
	}
}

// GetStats returns statistics about the thread manager
func (tm *ThreadManager) GetStats() ThreadManagerStats {
	stats := ThreadManagerStats{
		NodeID:       tm.nodeID,
		TotalThreads: tm.threads.Len(),
	}

	tm.threads.Range(func(name string, info *ThreadInfo) bool {
		if info.IsRunning {
			stats.RunningThreads++
		} else {
			stats.StoppedThreads++
		}
		return true
	})

	return stats
}

// ThreadManagerStats holds statistics about the thread manager
type ThreadManagerStats struct {
	NodeID         string
	TotalThreads   int
	RunningThreads int
	StoppedThreads int
}

// Helper function to extract thread names
func getThreadNames(threads map[string]*ThreadInfo) []string {
	names := make([]string, 0, len(threads))
	for name := range threads {
		names = append(names, name)
	}
	return names
}

// ThreadManagerOption allows configuration of the thread manager
type ThreadManagerOption func(*ThreadManager)

// WithShutdownTimeout sets the shutdown timeout
func WithShutdownTimeout(timeout time.Duration) ThreadManagerOption {
	return func(tm *ThreadManager) {
		tm.shutdownTimeout = timeout
	}
}

// WithLogger sets a custom logger
func WithLogger(logger *log.Logger) ThreadManagerOption {
	return func(tm *ThreadManager) {
		tm.logger = logger
	}
}

// NewThreadManagerWithOptions creates a thread manager with options
func NewThreadManagerWithOptions(nodeID string, options ...ThreadManagerOption) *ThreadManager {
	tm := NewThreadManager(nodeID, nil)
	for _, option := range options {
		option(tm)
	}
	return tm
}
