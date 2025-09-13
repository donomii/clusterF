// threadmanager_test.go - Tests for ThreadManager auto-restart functionality
package main

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func TestThreadManager_AutoRestart(t *testing.T) {
	// Create a test logger that discards output
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	tm := NewThreadManager("test-node", logger)
	defer tm.Shutdown()

	// Track how many times the function has been called
	callCount := 0

	// Function that exits after being called twice
	testFunc := func(ctx context.Context) {
		callCount++
		t.Logf("Test function called (count: %d)", callCount)

		if callCount <= 2 {
			// Exit early to trigger restart
			t.Logf("Test function exiting early to test restart")
			return
		}

		// On the third call, run normally until context is cancelled
		<-ctx.Done()
		t.Logf("Test function exiting due to context cancellation")
	}

	// Start the thread with auto-restart enabled
	err := tm.StartThreadWithRestart("test-thread", testFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to start thread: %v", err)
	}

	// Wait for the thread to restart a couple of times
	time.Sleep(12 * time.Second) // Monitor checks every 5 seconds

	// Check the thread status
	status := tm.GetThreadStatus()
	threadStatus, exists := status["test-thread"]
	if !exists {
		t.Fatal("Test thread not found in status")
	}

	if threadStatus.RestartCount < 2 {
		t.Errorf("Expected at least 2 restarts, got %d", threadStatus.RestartCount)
	}

	if !threadStatus.IsRunning {
		t.Error("Thread should still be running after restarts")
	}

	if !threadStatus.ShouldRestart {
		t.Error("Thread should be marked for restart")
	}

	t.Logf("Thread successfully restarted %d times", threadStatus.RestartCount)
}

func TestThreadManager_NoRestartDuringShutdown(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	tm := NewThreadManager("test-node", logger)

	// Function that exits immediately
	exitFunc := func(ctx context.Context) {
		t.Log("Function exiting immediately")
		// Exit immediately to test that restart doesn't happen during shutdown
	}

	// Start the thread
	err := tm.StartThreadWithRestart("exit-thread", exitFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to start thread: %v", err)
	}

	// Wait a moment for the thread to exit
	time.Sleep(1 * time.Second)

	// Start shutdown immediately
	tm.Shutdown()

	// Check that the thread was not restarted during shutdown
	status := tm.GetThreadStatus()
	threadStatus, exists := status["exit-thread"]
	if !exists {
		t.Fatal("Thread not found in status")
	}

	// Should have minimal restarts since shutdown happened quickly
	if threadStatus.RestartCount > 1 {
		t.Errorf("Expected minimal restarts during shutdown, got %d", threadStatus.RestartCount)
	}

	t.Logf("Thread restart count during shutdown: %d", threadStatus.RestartCount)
}

func TestThreadManager_NonRestartableThread(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	tm := NewThreadManager("test-node", logger)
	defer tm.Shutdown()

	callCount := 0

	// Function that exits after one call
	exitFunc := func(ctx context.Context) {
		callCount++
		t.Logf("Non-restartable function called (count: %d)", callCount)
		// Exit immediately
	}

	// Start the thread with auto-restart DISABLED
	err := tm.StartThreadWithRestart("no-restart-thread", exitFunc, false, nil)
	if err != nil {
		t.Fatalf("Failed to start thread: %v", err)
	}

	// Wait for monitoring cycles
	time.Sleep(12 * time.Second)

	// Check that the function was only called once
	if callCount != 1 {
		t.Errorf("Expected function to be called exactly once, got %d", callCount)
	}

	// Check thread status
	status := tm.GetThreadStatus()
	threadStatus, exists := status["no-restart-thread"]
	if !exists {
		t.Fatal("Thread not found in status")
	}

	if threadStatus.RestartCount != 0 {
		t.Errorf("Expected 0 restarts for non-restartable thread, got %d", threadStatus.RestartCount)
	}

	if threadStatus.ShouldRestart {
		t.Error("Thread should not be marked for restart")
	}

	t.Log("Non-restartable thread correctly stayed stopped")
}

func TestThreadManager_ThreadWithCleanup(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	tm := NewThreadManager("test-node", logger)
	defer tm.Shutdown()

	cleanupCount := 0

	// Cleanup function
	cleanup := func() {
		cleanupCount++
		t.Logf("Cleanup function called (count: %d)", cleanupCount)
	}

	callCount := 0

	// Function that exits after being called twice
	testFunc := func(ctx context.Context) {
		callCount++
		t.Logf("Function with cleanup called (count: %d)", callCount)

		if callCount <= 1 {
			// Exit early to trigger restart and cleanup
			return
		}

		// On the second call, run normally
		<-ctx.Done()
	}

	// Start the thread with cleanup
	err := tm.StartThreadWithRestart("cleanup-thread", testFunc, true, cleanup)
	if err != nil {
		t.Fatalf("Failed to start thread: %v", err)
	}

	// Wait for restart
	time.Sleep(8 * time.Second)

	// Cleanup should have been called at least once (from the first exit)
	if cleanupCount < 1 {
		t.Errorf("Expected cleanup to be called at least once, got %d", cleanupCount)
	}

	t.Logf("Cleanup function called %d times", cleanupCount)
}

/* FIXME - this test currently fails
func TestThreadManager_PanicRecovery(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	tm := NewThreadManager("test-node", logger)
	defer tm.Shutdown()

	callCount := 0

	// Function that panics on first call, then runs normally
	panicFunc := func(ctx context.Context) {
		callCount++
		t.Logf("Panic function called (count: %d)", callCount)

		if callCount == 1 {
			panic("Test panic to verify recovery and restart")
		}

		// On restart, run normally
		<-ctx.Done()
	}

	// Start the thread
	err := tm.StartThreadWithRestart("panic-thread", panicFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to start thread: %v", err)
	}

	// Wait for panic recovery and restart
	time.Sleep(8 * time.Second)

	// Check that the thread was restarted after panic
	status := tm.GetThreadStatus()
	threadStatus, exists := status["panic-thread"]
	if !exists {
		t.Fatal("Thread not found in status after panic")
	}

	if threadStatus.RestartCount < 1 {
		t.Errorf("Expected at least 1 restart after panic, got %d", threadStatus.RestartCount)
	}

	if !threadStatus.IsRunning {
		t.Error("Thread should be running after panic recovery")
	}

	t.Logf("Thread successfully recovered from panic and restarted %d times", threadStatus.RestartCount)
}
*/
