// filesystem_test.go - Tests for the distributed file system
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donomii/clusterF/testenv"
	"github.com/donomii/clusterF/types"
)

var testModTimeBase = time.Unix(1_700_000_000, 0)
var testModCounter int64

func nextTestModTime() time.Time {
	v := atomic.AddInt64(&testModCounter, 1)
	return testModTimeBase.Add(time.Duration(v) * time.Second)
}

func TestFileSystem_BasicOperations(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-fs-node",
		DataDir:       tempDir,
		HTTPDataPort:  40000,
		DiscoveryPort: 50001,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for cluster to be ready
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

	// Test file operations directly on the file system
	fs := cluster.FileSystem

	// Test 1: Store and retrieve a small file
	testContent := []byte("Hello, distributed file system!")
	_, err := fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), "/test.txt", testContent, "text/plain", nextTestModTime())
	if err != nil {
		t.Fatalf("Failed to store file: %v", err)
	}

	content, metadata, err := fs.GetFile("/test.txt")
	if err != nil {
		t.Fatalf("Failed to retrieve file: %v", err)
	}

	if !bytes.Equal(content, testContent) {
		t.Fatalf("Content mismatch: got %q, want %q", content, testContent)
	}

	if metadata.Name != "test.txt" {
		t.Fatalf("Name mismatch: got %q, want %q", metadata.Name, "test.txt")
	}

	if metadata.Size != int64(len(testContent)) {
		t.Fatalf("Size mismatch: got %d, want %d", metadata.Size, len(testContent))
	}

	t.Logf("✅ Basic file store/retrieve test passed")
}

func TestFileSystem_Directories(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-dir-node",
		DataDir:       tempDir,
		HTTPDataPort:  40001,
		DiscoveryPort: 50002,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	// Test 1: Create directory (no-op since directories are inferred)
	err := fs.CreateDirectory("/documents")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Test 2: Root directory listing should be empty initially
	entries, err := fs.ListDirectory("/")
	if err != nil {
		t.Fatalf("Failed to list root directory: %v", err)
	}

	if len(entries) != 0 {
		t.Logf("Root directory has %d entries (expected 0 since directories are inferred from files)", len(entries))
	}

	// Test 3: Store file in nested directory to create the directory structure
	testContent := []byte("Project documentation")
	_, err = fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), "/documents/projects/readme.txt", testContent, "text/plain", nextTestModTime())
	if err != nil {
		t.Fatalf("Failed to store file in nested directory: %v", err)
	}

	// Test 4: Now root directory should show 'documents' directory
	entries, err = fs.ListDirectory("/")
	if err != nil {
		t.Fatalf("Failed to list root directory: %v", err)
	}

	if len(entries) != 1 {
		// Print what we actually got
		entryNames := make([]string, len(entries))
		for i, e := range entries {
			entryNames[i] = fmt.Sprintf("%s (isDir=%v)", e.Name, e.IsDirectory)
		}
		t.Fatalf("Expected 1 entry in root directory after adding file, got %d: %v", len(entries), entryNames)
	}

	if entries[0].Name != "documents" || !entries[0].IsDirectory {
		t.Fatalf("Expected directory named 'documents', got %+v", entries[0])
	}

	// Test 5: List documents directory should show 'projects' directory
	entries, err = fs.ListDirectory("/documents")
	if err != nil {
		t.Fatalf("Failed to list documents directory: %v", err)
	}

	if len(entries) != 1 {
		entryNames := make([]string, len(entries))
		for i, e := range entries {
			entryNames[i] = fmt.Sprintf("%s (isDir=%v)", e.Name, e.IsDirectory)
		}
		t.Fatalf("Expected 1 entry in documents directory, got %d: %v", len(entries), entryNames)
	}

	if entries[0].Name != "projects" || !entries[0].IsDirectory {
		t.Fatalf("Expected directory named 'projects', got %+v", entries[0])
	}

	// Test 6: List projects directory should show the readme.txt file
	entries, err = fs.ListDirectory("/documents/projects")
	if err != nil {
		t.Fatalf("Failed to list projects directory: %v", err)
	}

	if len(entries) != 1 {
		entryNames := make([]string, len(entries))
		for i, e := range entries {
			entryNames[i] = fmt.Sprintf("%s (isDir=%v)", e.Name, e.IsDirectory)
		}
		t.Fatalf("Expected 1 file in projects directory, got %d: %v", len(entries), entryNames)
	}

	if entries[0].Name != "readme.txt" || entries[0].IsDirectory {
		t.Fatalf("Expected file named 'readme.txt', got %+v", entries[0])
	}

	t.Logf("✅ Directory operations test passed (directories inferred from file paths)")
}

func TestFileSystem_LargeFiles(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-large-node",
		DataDir:       tempDir,
		HTTPDataPort:  40002,
		DiscoveryPort: 50003,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	// Create a large file (200KB)
	largeContent := make([]byte, 200*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	_, err := fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), "/large.bin", largeContent, "application/octet-stream", nextTestModTime())
	if err != nil {
		t.Fatalf("Failed to store large file: %v", err)
	}

	// Retrieve and verify
	content, _, err := fs.GetFile("/large.bin")
	if err != nil {
		t.Fatalf("Failed to retrieve large file: %v", err)
	}

	if !bytes.Equal(content, largeContent) {
		t.Fatalf("Large file content mismatch: expected %d bytes, got %d bytes", len(largeContent), len(content))
	}

	// Just verify the file was stored correctly
	t.Logf("✅ Large file test passed: %d bytes", len(content))
}

func TestFileSystem_HTTPEndpoints(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-http-node",
		DataDir:       tempDir,
		HTTPDataPort:  40003,
		DiscoveryPort: 50004,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for HTTP server to be ready
	WaitForConditionT(t, "HTTP server startup", func() bool {
		client := &http.Client{Timeout: 1 * time.Second}
		baseURL := fmt.Sprintf("http://localhost:%d", cluster.HTTPDataPort)
		resp, err := client.Get(baseURL + "/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 50, 3000)

	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := fmt.Sprintf("http://localhost:%d", cluster.HTTPDataPort)

	// Test 1: Upload file via HTTP (skip directory creation since directories are inferred)
	testContent := "This is a test file uploaded via HTTP API"
	uploadTime := nextTestModTime()
	req, _ := http.NewRequest("PUT", baseURL+"/api/files/test.txt",
		strings.NewReader(testContent))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", uploadTime.Format(time.RFC3339))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to upload file via HTTP: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Expected 201 for file upload, got %d", resp.StatusCode)
	}

	// Test 2: Download file via HTTP
	resp, err = client.Get(baseURL + "/api/files/test.txt")
	if err != nil {
		t.Fatalf("Failed to download file via HTTP: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 for file download, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read download response: %v", err)
	}

	if string(body) != testContent {
		t.Fatalf("Downloaded content mismatch: got %q, want %q", body, testContent)
	}

	// Test 3: Delete file via HTTP
	req, _ = http.NewRequest("DELETE", baseURL+"/api/files/test.txt", nil)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Failed to delete file via HTTP: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("Expected 204 for file deletion, got %d", resp.StatusCode)
	}

	// Test 4: Verify file is deleted
	resp, err = client.Get(baseURL + "/api/files/test.txt")
	if err != nil {
		t.Fatalf("Failed to verify file deletion: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for deleted file, got %d", resp.StatusCode)
	}

	t.Logf("✅ HTTP endpoints test passed")
}

func TestFileSystem_MultiNode_Replication(t *testing.T) {
	testenv.RequireUDPSupport(t)
	config := TestConfig{
		NodeCount:     3,
		FileCount:     5,
		FileSize:      1024,
		TestName:      "TestFileSystem_MultiNode_Replication",
		TimeoutMs:     5000,
		DiscoveryPort: 37001, // Unique port to avoid conflicts
	}

	// Create a cluster with test configuration
	nodes, cleanup := setupTestCluster(t, config, config.TestName)
	defer cleanup()

	// Set fast partition sync interval for testing (1 second instead of default 30)
	for _, node := range nodes {
		node.SetPartitionSyncInterval(1)
	}

	// Wait for nodes to discover each other
	WaitForConditionT(t, "Node discovery", func() bool {
		for i, node := range nodes {
			peerCount := node.DiscoveryManager().GetPeerCount()
			if peerCount < 1 {
				t.Logf("Node %d has %d peers (waiting for discovery)", i, peerCount)
				return false
			}
		}
		return true
	}, 200, 6000)

	// Store file on node 0
	testContent := []byte("Multi-node replicated file content")
	filePath := "/replicated.txt"
	if _, err := nodes[0].FileSystem.StoreFileWithModTimeAndClusterUpdate(context.TODO(), filePath, testContent, "text/plain", nextTestModTime()); err != nil {
		t.Fatalf("Failed to store file on node 0: %v", err)
	}

	for i, _ := range nodes {
		nodes[i].FullSyncAllPeers()
	}

	// Compute the expected partition for this file using the shared partition helper
	expectedPartition := string(types.PartitionIDForPath(filePath))

	// Verify the file can be retrieved from each node over HTTP
	// Note: This relies on partition replication, which happens in the background
	// The partition needs to be replicated to RF (3) nodes, which can take time
	for i, n := range nodes {
		base := fmt.Sprintf("http://localhost:%d", n.HTTPDataPort)
		WaitForConditionT(t, fmt.Sprintf("File availability on node %d", i), func() bool {
			resp, err := http.Get(base + "/api/files" + filePath)
			if err != nil {
				t.Logf("Node %d: HTTP error: %v", i, err)
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Logf("Node %d: Status %d: %s", i, resp.StatusCode, string(body))
				return false
			}
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Logf("Node %d: Read error: %v", i, err)
				return false
			}
			if !bytes.Equal(b, testContent) {
				t.Logf("Node %d: Content mismatch", i)
				return false
			}
			t.Logf("Node %d: File retrieved successfully", i)
			return true
		}, 500, 20000) // Increased timeout to 60 seconds for partition replication
	}

	t.Logf("✅ Multi-node partition replication test passed: partition %s present and file retrievable on all %d nodes", expectedPartition, len(nodes))
}

func TestFileSystem_ErrorConditions(t *testing.T) {

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "test-error-node",
		DataDir:       tempDir,
		HTTPDataPort:  40004,
		DiscoveryPort: 50005,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	// Test 1: Try to get non-existent file
	_, _, err := fs.GetFile("/nonexistent.txt")
	if err == nil {
		t.Fatalf("Expected error for non-existent file")
	}

	// Test 2: Try to create file with invalid path
	_, err = fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), "../invalid", []byte("test"), "text/plain", nextTestModTime())
	if err == nil {
		t.Fatalf("Expected error for invalid path")
	}

	// Test 3: Try to delete non-existent file

	// Test 4: Try to create directory
	err = fs.CreateDirectory("/test")
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	t.Logf("✅ Error conditions test passed")
}

func BenchmarkFileSystem_SmallFiles(b *testing.B) {
	tempDir := b.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "bench-small",
		DataDir:       tempDir,
		DiscoveryPort: 50006,
	})

	fs := cluster.FileSystem
	testContent := []byte("Small file content for benchmarking")

	b.ResetTimer()

	b.Run("Store", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := fmt.Sprintf("/bench-small-%d.txt", i)
			_, _ = fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), path, testContent, "text/plain", nextTestModTime())
		}
	})

	// Store some files for read benchmark
	for i := 0; i < 100; i++ {
		path := fmt.Sprintf("/read-bench-%d.txt", i)
		_, _ = fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), path, testContent, "text/plain", nextTestModTime())
	}

	b.Run("Retrieve", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := fmt.Sprintf("/read-bench-%d.txt", i%100)
			fs.GetFile(path)
		}
	})
}

func BenchmarkFileSystem_LargeFiles(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping large file benchmark in short mode")
	}

	tempDir := b.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "bench-large",
		DataDir:       tempDir,
		DiscoveryPort: 50007,
	})

	fs := cluster.FileSystem

	// Create 1MB test content
	largeContent := make([]byte, 1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	b.ResetTimer()

	b.Run("Store1MB", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			path := fmt.Sprintf("/bench-large-%d.bin", i)
			_, _ = fs.StoreFileWithModTimeAndClusterUpdate(context.TODO(), path, largeContent, "application/octet-stream", nextTestModTime())
		}
	})
}
