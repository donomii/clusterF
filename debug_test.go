// debug_test.go - Minimal test to diagnose issues
package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestMinimal_CompilationCheck(t *testing.T) {
	t.Log("✅ Test compilation works")
}

func TestMinimal_NewCluster(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-node",
		DataDir:       tempDir,
		HTTPDataPort:  40000,
		DiscoveryPort: 57001,
	})
	if cluster == nil {
		t.Fatal("NewCluster returned nil")
	}

	t.Log("✅ NewCluster works")
}

func TestMinimal_StartStop(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-node",
		DataDir:       tempDir,
		HTTPDataPort:  40001,
		DiscoveryPort: 57002,
	})

	// Start the cluster
	cluster.Start()
	t.Log("✅ Cluster started")

	// FIXME: check the cluster has started, PROPERLY
	WaitForConditionT(t, "Cluster startup", func() bool {
		client := &http.Client{Timeout: 1 * time.Second}
		baseURL := fmt.Sprintf("http://localhost:%d", cluster.HTTPDataPort)
		resp, err := client.Get(baseURL + "/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 50, 2000)

	// Stop the cluster
	cluster.Stop()
	t.Log("✅ Cluster stopped")
}
