//go:build test
// +build test

package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donomii/clusterF/filesystem"
	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/types"
	ensemblekv "github.com/donomii/ensemblekv"
	"github.com/donomii/frogpond"
)

// MockClusterSearch implements the ClusterLike interface for testing
type MockClusterSearch struct {
	pm     *partitionmanager.PartitionManager
	logger *log.Logger
}

func (m *MockClusterSearch) PartitionManager() types.PartitionManagerLike {
	return m.pm
}

func (m *MockClusterSearch) DiscoveryManager() types.DiscoveryManagerLike {
	return nil
}

func (m *MockClusterSearch) Exporter() types.ExporterLike {
	return nil
}

func (m *MockClusterSearch) Logger() *log.Logger {
	return m.logger
}

func (m *MockClusterSearch) NoStore() bool {
	return false
}

func (m *MockClusterSearch) ListDirectoryUsingSearch(path string) ([]types.FileMetadata, error) {
	return []types.FileMetadata{}, nil
}

func (m *MockClusterSearch) DataClient() *http.Client {
	return &http.Client{}
}

func (m *MockClusterSearch) ID() types.NodeID {
	return types.NodeID("test-node-search")
}

func setupSearchTest(t *testing.T) (*filesystem.ClusterFileSystem, string) {
	tmpDir, err := os.MkdirTemp("", "search_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	logger := log.New(os.Stdout, "[SEARCH_TEST] ", log.LstdFlags)

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

	frogpond := frogpond.NewNode()

	deps := partitionmanager.Dependencies{
		NodeID:                types.NodeID("test-node-search"),
		NoStore:               false,
		Logger:                logger,
		Debugf:                func(format string, args ...interface{}) { logger.Printf(format, args...) },
		MetadataKV:            metadataKV,
		ContentKV:             contentKV,
		HTTPDataClient:        &http.Client{},
		Discovery:             nil,
		LoadPeer:              func(types.NodeID) (*types.PeerInfo, bool) { return nil, false },
		Frogpond:              frogpond,
		SendUpdatesToPeers:    func([]frogpond.DataPoint) {},
		NotifyFileListChanged: func() {},
		GetCurrentRF:          func() int { return 3 },
	}

	pm := partitionmanager.NewPartitionManager(deps)

	cluster := &MockClusterSearch{
		pm:     pm,
		logger: logger,
	}

	fs := filesystem.NewClusterFileSystem(cluster)

	return fs, tmpDir
}

func TestDeepDirectorySearch(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Create deep directory structure with files
	testFiles := []struct {
		path    string
		content string
	}{
		{"/root/file1.txt", "root level"},
		{"/root/dir1/file2.txt", "level 1"},
		{"/root/dir1/dir2/file3.txt", "level 2"},
		{"/root/dir1/dir2/dir3/file4.txt", "level 3"},
		{"/root/dir1/dir2/dir3/dir4/file5.txt", "level 4"},
		{"/root/dir1/dir2/dir3/dir4/dir5/file6.txt", "level 5"},
		{"/root/other/file7.txt", "other branch"},
		{"/root/other/sub/file8.txt", "other sub branch"},
	}

	t.Log("Creating deep directory structure...")
	for _, tf := range testFiles {
		err := fs.StoreFileWithModTime(tf.path, []byte(tf.content), "text/plain", time.Now())
		if err != nil {
			t.Fatalf("Failed to store %s: %v", tf.path, err)
		}
	}

	// Test listing at various levels
	tests := []struct {
		path          string
		expectedCount int
		description   string
	}{
		{"/root", 8, "root directory should show all files"},
		{"/root/dir1", 5, "dir1 should show 5 files in subdirectories"},
		{"/root/dir1/dir2", 4, "dir2 should show 4 files in subdirectories"},
		{"/root/dir1/dir2/dir3", 3, "dir3 should show 3 files in subdirectories"},
		{"/root/other", 2, "other should show 2 files"},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			results, err := fs.ListDirectory(tc.path)
			if err != nil {
				t.Fatalf("Failed to list %s: %v", tc.path, err)
			}
			if len(results) != tc.expectedCount {
				t.Errorf("Expected %d files in %s, got %d", tc.expectedCount, tc.path, len(results))
				for _, r := range results {
					t.Logf("  Found: %s", r.Name)
				}
			}
		})
	}
}

func TestLargeDirectoryListing(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Create directory with thousands of files
	numFiles := 2000
	t.Logf("Creating directory with %d files...", numFiles)

	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("/large/file%04d.dat", i)
		content := fmt.Sprintf("File number %d", i)
		err := fs.StoreFileWithModTime(path, []byte(content), "application/octet-stream", time.Now())
		if err != nil {
			t.Fatalf("Failed to store file %d: %v", i, err)
		}
	}

	t.Log("Listing large directory...")
	results, err := fs.ListDirectory("/large")
	if err != nil {
		t.Fatalf("Failed to list large directory: %v", err)
	}

	if len(results) != numFiles {
		t.Errorf("Expected %d files, got %d", numFiles, len(results))
	}

	// Verify all files are accounted for
	fileMap := make(map[string]bool)
	for _, r := range results {
		fileMap[r.Name] = true
	}

	for i := 0; i < numFiles; i++ {
		expectedName := fmt.Sprintf("/large/file%04d.dat", i)
		if !fileMap[expectedName] {
			t.Errorf("Missing file: %s", expectedName)
		}
	}
}

func TestFunnyFileNames(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Test various tricky filenames
	testFiles := []string{
		"/funny/file with spaces.txt",
		"/funny/file-with-dashes.txt",
		"/funny/file_with_underscores.txt",
		"/funny/file.multiple.dots.txt",
		"/funny/UPPERCASE.TXT",
		"/funny/MixedCase.TxT",
		"/funny/números-españoles.txt",
		"/funny/file!@#$%^&().txt",
		"/funny/file[brackets].txt",
		"/funny/file{braces}.txt",
		"/funny/file(parens).txt",
		"/funny/file'quote.txt",
		"/funny/file\"doublequote.txt",
		"/funny/file`backtick.txt",
		"/funny/file~tilde.txt",
		"/funny/very-long-filename-that-goes-on-and-on-and-on-to-test-length-limits.txt",
	}

	t.Log("Creating files with funny names...")
	for _, path := range testFiles {
		err := fs.StoreFileWithModTime(path, []byte("test content"), "text/plain", time.Now())
		if err != nil {
			t.Logf("Warning: Failed to store %s: %v", path, err)
			continue
		}
	}

	t.Log("Listing directory with funny filenames...")
	results, err := fs.ListDirectory("/funny")
	if err != nil {
		t.Fatalf("Failed to list funny directory: %v", err)
	}

	t.Logf("Found %d files with funny names", len(results))
	for _, r := range results {
		t.Logf("  %s", r.Name)
	}
}

func TestSearchPatterns(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Create files with different extensions and patterns
	testFiles := []struct {
		path    string
		content string
	}{
		{"/search/document1.txt", "text document"},
		{"/search/document2.txt", "text document"},
		{"/search/image1.jpg", "image data"},
		{"/search/image2.png", "image data"},
		{"/search/data.csv", "csv data"},
		{"/search/data.json", "json data"},
		{"/search/sub/nested1.txt", "nested text"},
		{"/search/sub/nested2.jpg", "nested image"},
	}

	t.Log("Creating search test files...")
	for _, tf := range testFiles {
		err := fs.StoreFileWithModTime(tf.path, []byte(tf.content), "application/octet-stream", time.Now())
		if err != nil {
			t.Fatalf("Failed to store %s: %v", tf.path, err)
		}
	}

	// Test different search scenarios
	tests := []struct {
		path        string
		description string
		shouldFind  []string
	}{
		{
			"/search",
			"all files in search",
			[]string{"document1.txt", "document2.txt", "image1.jpg", "image2.png", "data.csv", "data.json", "nested1.txt", "nested2.jpg"},
		},
		{
			"/search/sub",
			"files in subdirectory",
			[]string{"nested1.txt", "nested2.jpg"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			results, err := fs.ListDirectory(tc.path)
			if err != nil {
				t.Fatalf("Failed to search %s: %v", tc.path, err)
			}

			foundMap := make(map[string]bool)
			for _, r := range results {
				// Extract just the filename from the full path
				parts := strings.Split(r.Name, "/")
				filename := parts[len(parts)-1]
				foundMap[filename] = true
			}

			for _, expected := range tc.shouldFind {
				if !foundMap[expected] {
					t.Errorf("Expected to find %s but didn't", expected)
				}
			}
		})
	}
}

func TestEmptyDirectories(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Create one file to establish a directory structure
	err := fs.StoreFileWithModTime("/empty/sub/file.txt", []byte("only file"), "text/plain", time.Now())
	if err != nil {
		t.Fatalf("Failed to store file: %v", err)
	}

	// Now delete it to make the directory "empty" (in terms of having no files)
	err = fs.DeleteFile("/empty/sub/file.txt")
	if err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	// List the now-empty directory
	results, err := fs.ListDirectory("/empty/sub")
	if err != nil {
		t.Fatalf("Failed to list empty directory: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 files in empty directory, got %d", len(results))
	}
}

func TestListDirectoryRootPath(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	// Create files at various paths
	testFiles := []string{
		"/file1.txt",
		"/file2.txt",
		"/dir1/file3.txt",
		"/dir2/file4.txt",
	}

	t.Log("Creating root-level test files...")
	for _, path := range testFiles {
		err := fs.StoreFileWithModTime(path, []byte("content"), "text/plain", time.Now())
		if err != nil {
			t.Fatalf("Failed to store %s: %v", path, err)
		}
	}

	// List from root
	results, err := fs.ListDirectory("/")
	if err != nil {
		t.Fatalf("Failed to list root: %v", err)
	}

	if len(results) != 4 {
		t.Errorf("Expected 4 files from root, got %d", len(results))
		for _, r := range results {
			t.Logf("  Found: %s", r.Name)
		}
	}
}

func TestConcurrentDirectoryOperations(t *testing.T) {
	fs, tmpDir := setupSearchTest(t)
	defer os.RemoveAll(tmpDir)

	numGoroutines := 10
	filesPerGoroutine := 50

	t.Logf("Running concurrent operations: %d goroutines x %d files", numGoroutines, filesPerGoroutine)

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			for i := 0; i < filesPerGoroutine; i++ {
				path := fmt.Sprintf("/concurrent/g%d/file%d.txt", id, i)
				content := fmt.Sprintf("goroutine %d file %d", id, i)
				err := fs.StoreFileWithModTime(path, []byte(content), "text/plain", time.Now())
				if err != nil {
					t.Errorf("Goroutine %d failed to store file %d: %v", id, i, err)
				}
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all files are listed
	results, err := fs.ListDirectory("/concurrent")
	if err != nil {
		t.Fatalf("Failed to list concurrent directory: %v", err)
	}

	expectedFiles := numGoroutines * filesPerGoroutine
	if len(results) != expectedFiles {
		t.Errorf("Expected %d files, got %d", expectedFiles, len(results))
	}
}
