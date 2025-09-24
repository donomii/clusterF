// filesystem_test.go - Tests for the distributed file system
package filesystem

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/donomii/clusterF/discovery"
)

// Mock implementations for testing

type mockLogger struct{}

func (l *mockLogger) Printf(format string, v ...interface{}) {}

type mockPartitionManager struct {
	files map[string][]byte
	metadata map[string]map[string]interface{}
}

func newMockPartitionManager() *mockPartitionManager {
	return &mockPartitionManager{
		files: make(map[string][]byte),
		metadata: make(map[string]map[string]interface{}),
	}
}

func (m *mockPartitionManager) StoreFileInPartition(path string, metadataJSON []byte, content []byte) error {
	m.files[path] = content
	var metadata map[string]interface{}
	if err := json.Unmarshal(metadataJSON, &metadata); err == nil {
		m.metadata[path] = metadata
	}
	return nil
}

func (m *mockPartitionManager) GetFileAndMetaFromPartition(path string) ([]byte, map[string]interface{}, error) {
	content, exists := m.files[path]
	if !exists {
		return nil, nil, ErrFileNotFound
	}
	metadata, exists := m.metadata[path]
	if !exists {
		metadata = make(map[string]interface{})
	}
	return content, metadata, nil
}

func (m *mockPartitionManager) GetMetadataFromPartition(path string) (map[string]interface{}, error) {
	metadata, exists := m.metadata[path]
	if !exists {
		return nil, ErrFileNotFound
	}
	return metadata, nil
}

func (m *mockPartitionManager) DeleteFileFromPartition(path string) error {
	delete(m.files, path)
	delete(m.metadata, path)
	return nil
}

type mockDiscoveryManager struct{}

func (m *mockDiscoveryManager) GetPeers() []*discovery.PeerInfo {
	return nil
}

func TestFileSystem_BasicOperations(t *testing.T) {
	deps := Dependencies{
		Logger:           &mockLogger{},
		PartitionManager: newMockPartitionManager(),
		DiscoveryManager: &mockDiscoveryManager{},
		NoStore:          false,
	}

	fs := NewClusterFileSystem(deps)

	// Test 1: Store and retrieve a small file
	testContent := []byte("Hello, distributed file system!")
	modTime := time.Now()
	
	err := fs.StoreFileWithModTime("/test.txt", testContent, "text/plain", modTime)
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

func TestFileSystem_ErrorConditions(t *testing.T) {
	deps := Dependencies{
		Logger:           &mockLogger{},
		PartitionManager: newMockPartitionManager(),
		DiscoveryManager: &mockDiscoveryManager{},
		NoStore:          false,
	}

	fs := NewClusterFileSystem(deps)

	// Test 1: Try to get non-existent file
	_, _, err := fs.GetFile("/nonexistent.txt")
	if err == nil {
		t.Fatalf("Expected error for non-existent file")
	}

	// Test 2: Try to create file with invalid path
	err = fs.StoreFileWithModTime("../invalid", []byte("test"), "text/plain", time.Now())
	if err == nil {
		t.Fatalf("Expected error for invalid path")
	}

	// Test 3: Try to delete non-existent file
	err = fs.DeleteFile("/nonexistent.txt")
	if err == nil {
		t.Fatalf("Expected error for deleting non-existent file")
	}

	t.Logf("✅ Error conditions test passed")
}
