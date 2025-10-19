// test_checksums.go - Simple test to verify checksum functionality
//go:build test
// +build test

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/donomii/clusterF/filesystem"
	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/types"
	ensemblekv "github.com/donomii/ensemblekv"
	"github.com/donomii/frogpond"
)

// MockCluster implements the ClusterLike interface for testing
type MockCluster struct {
	pm     *partitionmanager.PartitionManager
	logger *log.Logger
}

func (m *MockCluster) PartitionManager() types.PartitionManagerLike {
	return m.pm
}

func (m *MockCluster) DiscoveryManager() types.DiscoveryManagerLike {
	return nil // Not needed for this test
}

func (m *MockCluster) Exporter() types.ExporterLike {
	return nil // Not needed for this test
}

func (m *MockCluster) Logger() *log.Logger {
	return m.logger
}

func (m *MockCluster) NoStore() bool {
	return false
}

func (m *MockCluster) ListDirectoryUsingSearch(path string) ([]types.FileMetadata, error) {
	return []types.FileMetadata{}, nil // Not needed for this test
}

func (m *MockCluster) DataClient() *http.Client {
	return &http.Client{}
}

func (m *MockCluster) ID() types.NodeID {
	return types.NodeID("test-node")
}

func TestChecksumFunctionality(t *testing.T) {
	// Create temporary directory for test data
	tmpDir, err := os.MkdirTemp("", "checksum_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize logger
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Initialize KV stores
	metadataKVPath := filepath.Join(tmpDir, "metadata")
	contentKVPath := filepath.Join(tmpDir, "content")

	metadataKV := ensemblekv.SimpleEnsembleCreator("extent", "", metadataKVPath, 8*1024*1024, 32, 256*1024*1024)
	contentKV := ensemblekv.SimpleEnsembleCreator("extent", "", contentKVPath, 2*1024*1024, 16, 64*1024*1024)

	if metadataKV == nil {
		t.Fatalf("Failed to create metadata KV store")
	}
	if contentKV == nil {
		t.Fatalf("Failed to create content KV store")
	}

	// Initialize frogpond
	frogpond := frogpond.NewNode()

	// Initialize partition manager
	deps := partitionmanager.Dependencies{
		NodeID:                types.NodeID("test-node"),
		NoStore:               false,
		Logger:                logger,
		Debugf:                func(format string, args ...interface{}) { logger.Printf(format, args...) },
		MetadataKV:            metadataKV,
		ContentKV:             contentKV,
		HttpDataClient:        &http.Client{},
		Discovery:             nil,
		LoadPeer:              func(types.NodeID) (*types.PeerInfo, bool) { return nil, false },
		Frogpond:              frogpond,
		SendUpdatesToPeers:    func([]frogpond.DataPoint) {},
		NotifyFileListChanged: func() {},
		GetCurrentRF:          func() int { return 3 },
	}

	pm := partitionmanager.NewPartitionManager(deps)

	// Initialize mock cluster
	cluster := &MockCluster{
		pm:     pm,
		logger: logger,
	}

	// Initialize file system
	fs := filesystem.NewClusterFileSystem(cluster, false)

	// Test data
	testPath := "/test/file.txt"
	testContent := []byte("Hello, World! This is a test file for checksum verification.")
	testContentType := "text/plain"
	testModTime := time.Now()

	// Test 1: Store file with checksum
	t.Log("Test 1: Storing file with checksum")
	_, err = fs.StoreFileWithModTime(context.TODO(), testPath, testContent, testContentType, testModTime)
	if err != nil {
		t.Fatalf("Failed to store file: %v", err)
	}
	t.Log("✓ File stored successfully")

	// Test 2: Retrieve file and verify checksum
	t.Log("Test 2: Retrieving file and verifying checksum")
	retrievedContent, metadata, err := fs.GetFile(testPath)
	if err != nil {
		t.Fatalf("Failed to retrieve file: %v", err)
	}

	if string(retrievedContent) != string(testContent) {
		t.Fatalf("Retrieved content doesn't match original")
	}

	if metadata.Checksum == "" {
		t.Fatalf("No checksum found in metadata")
	}
	t.Logf("✓ File retrieved successfully with checksum: %s", metadata.Checksum)

	// Test 3: Verify integrity check
	t.Log("Test 3: Running integrity check")
	integrityResults := pm.VerifyStoredFileIntegrity()
	t.Logf("✓ Integrity check results: %+v", integrityResults)

	// Verify results
	if integrityResults["verified"] != 1 {
		t.Fatalf("Expected 1 verified file, got %v", integrityResults["verified"])
	}

	if integrityResults["corrupted"] != 0 {
		t.Fatalf("Expected 0 corrupted files, got %v", integrityResults["corrupted"])
	}

	t.Log("✓ All checksum tests passed!")
}
