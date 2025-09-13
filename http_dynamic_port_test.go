// http_dynamic_port_test.go - Test dynamic port allocation for HTTP server
package main

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestHTTPServer_DynamicPortAllocation(t *testing.T) {
	tempDir := t.TempDir()

	// Create multiple clusters that will compete for ports
	clusters := make([]*Cluster, 5)

	for i := 0; i < 5; i++ {
		clusters[i] = NewCluster(ClusterOpts{
			ID:            fmt.Sprintf("port-test-node-%d", i),
			DataDir:       filepath.Join(tempDir, fmt.Sprintf("node%d", i)),
			UDPListenPort: 23000 + i,
			HTTPDataPort:  32000, // Same port for all - should cause conflicts
			DiscoveryPort: 19400 + i,
		})

		clusters[i].Start()

		// Small delay to stagger startup
		time.Sleep(100 * time.Millisecond)
	}

	// Cleanup
	defer func() {
		for _, cluster := range clusters {
			if cluster != nil {
				cluster.Stop()
			}
		}
	}()

	// Wait for all clusters to start
	time.Sleep(3 * time.Second)

	// Verify each cluster got a working HTTP port
	usedPorts := make(map[int]bool)

	for i, cluster := range clusters {
		port := cluster.HTTPDataPort

		// Check that port is unique
		if usedPorts[port] {
			t.Errorf("Cluster %d got duplicate port %d", i, port)
		}
		usedPorts[port] = true

		// Check that port is accessible
		client := &http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(fmt.Sprintf("http://localhost:%d/status", port))
		if err != nil {
			t.Errorf("Cluster %d failed to respond on port %d: %v", i, port, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Cluster %d returned status %d on port %d", i, resp.StatusCode, port)
		}

		t.Logf("Cluster %d successfully running on port %d", i, port)
	}

	// Verify all ports are different
	if len(usedPorts) != len(clusters) {
		t.Errorf("Expected %d unique ports, got %d", len(clusters), len(usedPorts))
	}

	t.Logf("All %d clusters successfully allocated unique ports", len(clusters))
}

func TestHTTPServer_RestartWithNewPort(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "restart-test-node",
		DataDir:       filepath.Join(tempDir, "restart-test"),
		UDPListenPort: 23100,
		HTTPDataPort:  32100,
		DiscoveryPort: 19600,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for initial startup
	time.Sleep(2 * time.Second)

	initialPort := cluster.HTTPDataPort
	t.Logf("Initial HTTP port: %d", initialPort)

	// Stop the HTTP server thread to simulate a failure
	cluster.ThreadManager.StopThread("http-server", 10*time.Second)

	// Wait for thread to stop and restart
	time.Sleep(8 * time.Second)

	newPort := cluster.HTTPDataPort
	t.Logf("Port after restart: %d", newPort)

	// Verify the server is accessible on the new port
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost:%d/status", newPort))
	if err != nil {
		t.Fatalf("Failed to connect to restarted server on port %d: %v", newPort, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	// Check thread restart count
	status := cluster.ThreadManager.GetThreadStatus()
	httpStatus, exists := status["http-server"]
	if !exists {
		t.Fatal("HTTP server thread not found in status")
	}

	if httpStatus.RestartCount < 1 {
		t.Errorf("Expected at least 1 restart, got %d", httpStatus.RestartCount)
	}

	t.Logf("HTTP server successfully restarted %d times and is accessible on port %d",
		httpStatus.RestartCount, newPort)
}

func TestHTTPServer_PortRangeExhaustion(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "exhaustion-test-node",
		DataDir:       filepath.Join(tempDir, "exhaustion-test"),
		UDPListenPort: 23200,
		HTTPDataPort:  0, // Force random port selection
		DiscoveryPort: 19700,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for startup
	time.Sleep(2 * time.Second)

	port := cluster.HTTPDataPort

	// Verify port is in expected range (30000+)
	if port < 30000 {
		t.Errorf("Expected port >= 30000, got %d", port)
	}

	// Verify server is accessible
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost:%d/status", port))
	if err != nil {
		t.Fatalf("Failed to connect to server on random port %d: %v", port, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200, got %d", resp.StatusCode)
	}

	t.Logf("Server successfully allocated random port %d", port)
}
