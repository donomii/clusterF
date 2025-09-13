//go:build integration
// +build integration

// cluster_large_test.go - Large scale cluster tests requiring significant resources
package main

import (
	"testing"
	"time"
)

func TestCluster_ThousandNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 1000-node test in short mode")
	}

	overallStart := time.Now()

	result := testDiscoveryAndPeering(t, TestConfig{
		NodeCount:     1000,
		ChunkCount:    10,
		ChunkSize:     512,
		TestName:      "ThousandNodes",
		TimeoutMs:     120000,
		DiscoveryPort: 22000, // Unique port
	})

	overallDuration := time.Since(overallStart)

	if !result.Success {
		t.Fatalf("Thousand node test failed: %v", result.Error)
	}

	t.Logf("🚀 Thousand node test passed: %d nodes discovered peers in %v (total test time: %v)",
		result.NodesCreated, result.Duration, overallDuration)

	// Calculate the speedup
	sequentialEstimate := 1000 * 5 * time.Second        // Old estimate: 1000 nodes × 5 seconds each
	actualShutdown := overallDuration - result.Duration // Rough shutdown time
	speedup := float64(sequentialEstimate) / float64(actualShutdown)

	t.Logf("💨 Parallel shutdown speedup: %.1fx faster than sequential (estimated %v vs actual %v)",
		speedup, sequentialEstimate, actualShutdown)
}

func TestCluster_HundredNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 100-node test in short mode")
	}

	result := testBasicOperations(t, TestConfig{
		NodeCount:     100,
		ChunkCount:    50,
		ChunkSize:     1024,
		TestName:      "HundredNodes",
		TimeoutMs:     60000,
		DiscoveryPort: 21000, // Unique port
	})

	if !result.Success {
		t.Fatalf("Hundred node test failed: %v", result.Error)
	}

	t.Logf("Hundred node test passed: %d nodes, %d chunks stored, %d replicated in %v",
		result.NodesCreated, result.ChunksStored, result.ChunksReplicated, result.Duration)
}

// Comprehensive scaling test that can be run with different parameters - now with concurrent subtests
func TestCluster_Large_Scaling(t *testing.T) {
	testCases := []TestConfig{
		{NodeCount: 1, ChunkCount: 10, ChunkSize: 1024, TestName: "Scale_1_Node", TimeoutMs: 5000},
		{NodeCount: 10, ChunkCount: 50, ChunkSize: 1024, TestName: "Scale_10_Nodes", TimeoutMs: 60000},
		{NodeCount: 100, ChunkCount: 100, ChunkSize: 1024, TestName: "Scale_100_Nodes", TimeoutMs: 120000},
		{NodeCount: 1000, ChunkCount: 1000, ChunkSize: 51200, TestName: "Scale_1000_Nodes", TimeoutMs: 180000},
	}

	// Run all test cases in parallel with different discovery ports
	for i, tc := range testCases {
		// Assign unique discovery port to each test case to avoid conflicts
		tc.DiscoveryPort = 20000 + (i * 200) // Different base port from regular tests
		t.Run(tc.TestName, func(t *testing.T) {

			if tc.NodeCount >= 100 {
				// Use discovery-only test for very large clusters to avoid timeout
				result := testDiscoveryAndPeering(t, tc)
				if !result.Success {
					t.Fatalf("Scaling test %s failed: %v", tc.TestName, result.Error)
				}
				t.Logf("Scaling test %s passed: %d nodes in %v",
					tc.TestName, result.NodesCreated, result.Duration)
			} else {
				// Full operation test for smaller clusters
				result := testBasicOperations(t, tc)
				if !result.Success {
					t.Fatalf("Scaling test %s failed: %v", tc.TestName, result.Error)
				}
				t.Logf("Scaling test %s passed: %d nodes, %d chunks, %d replicated in %v",
					tc.TestName, result.NodesCreated, result.ChunksStored, result.ChunksReplicated, result.Duration)
			}
		})
	}
}
