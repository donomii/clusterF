package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func TestPerformLocalSearch(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "search-test-node",
		DataDir:       tempDir,
		HTTPDataPort:  40100,
		DiscoveryPort: 50100,
		Debug:         true,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	testFiles := map[string][]byte{
		"/documents/report.txt":      []byte("Test report"),
		"/documents/summary.txt":     []byte("Test summary"),
		"/documents/archive/old.txt": []byte("Old file"),
		"/images/photo.jpg":          []byte("Photo data"),
		"/images/screenshot.png":     []byte("Screenshot data"),
	}

	now := time.Now()
	for path, content := range testFiles {
		if _, err := fs.InsertFileIntoCluster(context.TODO(), path, content, "text/plain", now); err != nil {
			t.Fatalf("Failed to store test file %s: %v", path, err)
		}
	}

	tests := []struct {
		name          string
		query         string
		expectedCount int
		expectedPaths []string
	}{
		{
			name:          "Search documents prefix",
			query:         "/documents/",
			expectedCount: 3,
			expectedPaths: []string{"/documents/report.txt", "/documents/summary.txt", "/documents/archive/"},
		},
		{
			name:          "Search images prefix",
			query:         "/images/",
			expectedCount: 2,
			expectedPaths: []string{"/images/photo.jpg", "/images/screenshot.png"},
		},
		{
			name:          "Search specific file",
			query:         "/documents/report.txt",
			expectedCount: 1,
			expectedPaths: []string{"/documents/report.txt"},
		},
		{
			name:          "Search nested directory",
			query:         "/documents/archive/",
			expectedCount: 1,
			expectedPaths: []string{"/documents/archive/old.txt"},
		},
		{
			name:          "Search root",
			query:         "/",
			expectedCount: 2,
			expectedPaths: []string{"/documents/", "/images/"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := SearchRequest{
				Query: tt.query,
			}

			results := cluster.performLocalSearch(req)

			for _, result := range results {
				if len(result.Holders) == 0 {
					t.Fatalf("result %s missing holders", result.Path)
				}
				found := false
				for _, holder := range result.Holders {
					if holder == cluster.ID() {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("result %s missing local holder %s in %+v", result.Path, cluster.ID(), result.Holders)
				}
			}

			if len(results) != tt.expectedCount {
				t.Errorf("Expected %d results, got %d", tt.expectedCount, len(results))
			}

			resultPaths := make(map[string]bool)
			for _, result := range results {
				resultPaths[result.Path] = true
			}

			for _, expectedPath := range tt.expectedPaths {
				if !resultPaths[expectedPath] {
					t.Errorf("Expected path %s not found in results", expectedPath)
				}
			}
		})
	}
}

func TestPerformLocalSearch_DeletedFiles(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "search-deleted-test",
		DataDir:       tempDir,
		HTTPDataPort:  40101,
		DiscoveryPort: 50101,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	filePath := "/test-file.txt"
	if _, err := fs.InsertFileIntoCluster(context.TODO(), filePath, []byte("test content"), "text/plain", time.Now()); err != nil {
		t.Fatalf("Failed to store test file: %v", err)
	}

	req := SearchRequest{Query: "/"}
	results := cluster.performLocalSearch(req)

	if len(results) != 1 {
		t.Fatalf("Expected 1 result before deletion, got %d", len(results))
	}

	if err := fs.DeleteFile(context.TODO(), filePath); err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	results = cluster.performLocalSearch(req)

	if len(results) != 0 {
		t.Errorf("Expected 0 results after deletion, got %d", len(results))
	}
}

func TestScanAllFiles(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "scan-test-node",
		DataDir:       tempDir,
		HTTPDataPort:  40102,
		DiscoveryPort: 50102,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	testFiles := map[string][]byte{
		"/file1.txt": []byte("Content 1"),
		"/file2.txt": []byte("Content 2"),
		"/file3.txt": []byte("Content 3"),
	}

	for path, content := range testFiles {
		if _, err := fs.InsertFileIntoCluster(context.TODO(), path, content, "text/plain", time.Now()); err != nil {
			t.Fatalf("Failed to store test file %s: %v", path, err)
		}
	}

	scannedFiles := make(map[string]types.FileMetadata)

	err := cluster.partitionManager.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		scannedFiles[filePath] = metadata
		return nil
	})

	if err != nil {
		t.Fatalf("ScanAllFiles failed: %v", err)
	}

	if len(scannedFiles) != len(testFiles) {
		t.Errorf("Expected %d scanned files, got %d", len(testFiles), len(scannedFiles))
	}

	for path := range testFiles {
		if _, found := scannedFiles[path]; !found {
			t.Errorf("File %s not found in scan results", path)
		}
	}

	for path, metadata := range scannedFiles {
		if metadata.ModifiedAt.IsZero() {
			t.Errorf("File %s has zero ModifiedAt", path)
		}
		if metadata.Size == 0 {
			t.Errorf("File %s has zero size", path)
		}
		if metadata.Name != filepath.Base(path) {
			t.Errorf("File %s has incorrect name: %s", path, metadata.Name)
		}
	}
}

func TestScanAllFiles_WithError(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "scan-error-test",
		DataDir:       tempDir,
		HTTPDataPort:  40103,
		DiscoveryPort: 50103,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	if _, err := fs.InsertFileIntoCluster(context.TODO(), "/file1.txt", []byte("Content 1"), "text/plain", time.Now()); err != nil {
		t.Fatalf("Failed to store test file: %v", err)
	}
	if _, err := fs.InsertFileIntoCluster(context.TODO(), "/file2.txt", []byte("Content 2"), "text/plain", time.Now()); err != nil {
		t.Fatalf("Failed to store test file: %v", err)
	}

	count := 0
	err := cluster.partitionManager.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		count++
		return nil
	})

	if err != nil {
		t.Fatalf("ScanAllFiles failed: %v", err)
	}

	if count != 2 {
		t.Errorf("Expected 2 files scanned, got %d", count)
	}
}

func TestScanMetadata(t *testing.T) {
	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "scan-metadata-test",
		DataDir:       tempDir,
		HTTPDataPort:  40104,
		DiscoveryPort: 50104,
	})

	cluster.Start()
	defer cluster.Stop()

	fs := cluster.FileSystem

	testFiles := map[string]struct {
		content     []byte
		contentType string
	}{
		"/doc.txt":   {[]byte("Document"), "text/plain"},
		"/data.bin":  {[]byte("Binary data"), "application/octet-stream"},
		"/info.json": {[]byte(`{"key":"value"}`), "application/json"},
	}

	for path, data := range testFiles {
		if _, err := fs.InsertFileIntoCluster(context.TODO(), path, data.content, data.contentType, time.Now()); err != nil {
			t.Fatalf("Failed to store test file %s: %v", path, err)
		}
	}

	scannedMetadata := make(map[string]types.FileMetadata)

	err := cluster.partitionManager.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		scannedMetadata[filePath] = metadata
		return nil
	})

	if err != nil {
		t.Fatalf("ScanAllFiles failed: %v", err)
	}

	for path, expected := range testFiles {
		metadata, found := scannedMetadata[path]
		if !found {
			t.Errorf("Metadata for %s not found", path)
			continue
		}

		if metadata.ContentType != expected.contentType {
			t.Errorf("File %s: expected content type %s, got %s", path, expected.contentType, metadata.ContentType)
		}

		if metadata.Size != int64(len(expected.content)) {
			t.Errorf("File %s: expected size %d, got %d", path, len(expected.content), metadata.Size)
		}

		if metadata.Checksum == "" {
			t.Errorf("File %s: checksum is empty", path)
		}

		if metadata.ModifiedAt.IsZero() {
			t.Errorf("File %s: ModifiedAt is zero", path)
		}

		if metadata.CreatedAt.IsZero() {
			t.Errorf("File %s: CreatedAt is zero", path)
		}
	}
}
