// files_api_direct_test.go - Direct function tests for file API handlers
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/donomii/clusterF/testenv"
	"github.com/donomii/clusterF/types"
)

// TestFileAPI_Direct tests handleFileGetInternal and handleFilePut directly without HTTP server
func TestFileAPI_Direct(t *testing.T) {
	testenv.RequireUDPSupport(t)

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "direct-test",
		DataDir:       tempDir,
		DiscoveryPort: 29001,
		Debug:         true,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for the cluster to be ready
	WaitForConditionT(t, "Cluster initialization", func() bool {
		return cluster.FileSystem != nil && cluster.PartitionManager() != nil
	}, 100, 5000)

	testData := []byte("Hello, direct test world!")
	testPath := "/direct-test-file.txt"
	contentType := "text/plain"
	modTime := time.Now()

	// Test direct PUT operation
	t.Run("DirectPUT", func(t *testing.T) {
		// Create a request body with test data
		body := bytes.NewReader(testData)

		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodPut, "/api/files"+testPath, body)
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("X-ClusterF-Modified-At", modTime.Format(time.RFC3339))

		w := httptest.NewRecorder()

		// Call handleFilePut directly
		cluster.handleFilePut(w, req, testPath)

		// Check response
		if w.Code != http.StatusCreated {
			t.Fatalf("Expected status 201, got %d. Response: %s", w.Code, w.Body.String())
		}

		t.Logf("PUT response: %s", w.Body.String())

		// Rescan partition to register file metadata in CRDT
		partitionID := cluster.PartitionManager().CalculatePartitionName(testPath)
		t.Logf("Rescanning partition %s for file %s", partitionID, testPath)
		// Use the correct method name - updatePartitionMetadata is private, so trigger reindex instead
		cluster.PartitionManager().MarkForReindex(types.PartitionID(partitionID))
		cluster.PartitionManager().RunReindex(context.Background())
		t.Logf("Partition rescan completed")
	})

	// Test direct GET operation (internal)
	t.Run("DirectGETInternal", func(t *testing.T) {
		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodGet, "/api/files"+testPath, nil)
		req.Header.Set("X-ClusterF-Internal", "1")

		w := httptest.NewRecorder()

		WaitForConditionT(t, "Waiting for crdt update after file insertion", func() bool {
			w = httptest.NewRecorder()
			// Call handleFileGetInternal directly
			cluster.handleFileGetInternal(w, req, testPath)

			// Check response
			if w.Code != http.StatusOK {
				return false
			}
			return true
		}, 200, 10000)

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}
		// Check response body
		responseData := w.Body.Bytes()
		if !bytes.Equal(responseData, testData) {
			t.Fatalf("Response data mismatch. Expected %q, got %q", testData, responseData)
		}

		// Check headers
		if ct := w.Header().Get("Content-Type"); ct != contentType {
			t.Errorf("Expected Content-Type %q, got %q", contentType, ct)
		}

		if modHeader := w.Header().Get("X-ClusterF-Modified-At"); modHeader == "" {
			t.Error("Missing X-ClusterF-Modified-At header")
		}

		if checksum := w.Header().Get("X-ClusterF-Checksum"); checksum == "" {
			t.Error("Missing X-ClusterF-Checksum header")
		}

		t.Logf("GET response headers: %v", w.Header())
		t.Logf("GET response data: %q", responseData)
	})

	// Test direct GET operation (external)
	t.Run("DirectGETExternal", func(t *testing.T) {
		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodGet, "/api/files"+testPath, nil)
		// No X-ClusterF-Internal header means external request

		w := httptest.NewRecorder()

		// Call handleFileGet directly (the external version)
		cluster.handleFileGet(w, req, testPath)

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Check response body
		responseData := w.Body.Bytes()
		if !bytes.Equal(responseData, testData) {
			t.Fatalf("Response data mismatch. Expected %q, got %q", testData, responseData)
		}

		t.Logf("External GET response headers: %v", w.Header())
		t.Logf("External GET response data: %q", responseData)
	})

	// Test direct HEAD operation
	t.Run("DirectHEAD", func(t *testing.T) {
		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodHead, "/api/files"+testPath, nil)

		w := httptest.NewRecorder()

		// Call handleFileHead directly
		cluster.handleFileHead(w, req, testPath)

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Check headers (HEAD should have no body but all metadata headers)
		if ct := w.Header().Get("Content-Type"); ct != contentType {
			t.Errorf("Expected Content-Type %q, got %q", contentType, ct)
		}

		if modHeader := w.Header().Get("X-ClusterF-Modified-At"); modHeader == "" {
			t.Error("Missing X-ClusterF-Modified-At header")
		}

		if checksum := w.Header().Get("X-ClusterF-Checksum"); checksum == "" {
			t.Error("Missing X-ClusterF-Checksum header")
		}

		if isDir := w.Header().Get("X-ClusterF-Is-Directory"); isDir != "false" {
			t.Errorf("Expected X-ClusterF-Is-Directory to be 'false', got %q", isDir)
		}

		// HEAD should have no body
		if w.Body.Len() != 0 {
			t.Errorf("HEAD response should have no body, got %d bytes", w.Body.Len())
		}

		t.Logf("HEAD response headers: %v", w.Header())
	})

	// Test direct DELETE operation
	t.Run("DirectDELETE", func(t *testing.T) {
		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodDelete, "/api/files"+testPath, nil)

		w := httptest.NewRecorder()

		// Call handleFileDelete directly
		cluster.handleFileDelete(w, req, testPath)

		// Check response
		if w.Code != http.StatusNoContent {
			t.Fatalf("Expected status 204, got %d. Response: %s", w.Code, w.Body.String())
		}

		// DELETE should have no body
		if w.Body.Len() != 0 {
			t.Errorf("DELETE response should have no body, got %d bytes", w.Body.Len())
		}

		t.Logf("DELETE response: status %d", w.Code)

		// Trigger reindex to update CRDT with deletion
		partitionID := cluster.PartitionManager().CalculatePartitionName(testPath)
		cluster.PartitionManager().MarkForReindex(types.PartitionID(partitionID))
		cluster.PartitionManager().RunReindex(context.Background())
	})

	// Test GET after DELETE should return 404
	t.Run("DirectGETAfterDelete", func(t *testing.T) {
		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodGet, "/api/files"+testPath, nil)
		req.Header.Set("X-ClusterF-Internal", "1")

		w := httptest.NewRecorder()

		// Call handleFileGetInternal directly
		cluster.handleFileGetInternal(w, req, testPath)

		// Check response
		if w.Code != http.StatusNotFound {
			t.Fatalf("Expected status 404 after DELETE, got %d. Response: %s", w.Code, w.Body.String())
		}

		responseBody := w.Body.String()
		if !strings.Contains(responseBody, "File not found") {
			t.Errorf("Expected 'File not found' in response, got: %s", responseBody)
		}

		t.Logf("GET after DELETE response: %s", responseBody)
	})

	// Test GET for non-existent file
	t.Run("DirectGETNotFound", func(t *testing.T) {
		nonExistentPath := "/does-not-exist.txt"

		// Create HTTP request and response recorder
		req := httptest.NewRequest(http.MethodGet, "/api/files"+nonExistentPath, nil)
		req.Header.Set("X-ClusterF-Internal", "1")

		w := httptest.NewRecorder()

		// Call handleFileGetInternal directly
		cluster.handleFileGetInternal(w, req, nonExistentPath)

		// Check response
		if w.Code != http.StatusNotFound {
			t.Fatalf("Expected status 404, got %d. Response: %s", w.Code, w.Body.String())
		}

		responseBody := w.Body.String()
		if !strings.Contains(responseBody, "File not found") {
			t.Errorf("Expected 'File not found' in response, got: %s", responseBody)
		}

		t.Logf("404 response: %s", responseBody)
	})

	// Test PUT with missing required headers
	t.Run("DirectPUTMissingHeaders", func(t *testing.T) {
		body := bytes.NewReader(testData)

		// Create HTTP request without required X-ClusterF-Modified-At header
		req := httptest.NewRequest(http.MethodPut, "/api/files/missing-header.txt", body)
		req.Header.Set("Content-Type", contentType)
		// Missing: X-ClusterF-Modified-At header

		w := httptest.NewRecorder()

		// Call handleFilePut directly
		cluster.handleFilePut(w, req, "/missing-header.txt")

		// Check response
		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected status 400, got %d. Response: %s", w.Code, w.Body.String())
		}

		responseBody := w.Body.String()
		if !strings.Contains(responseBody, "Missing X-ClusterF-Modified-At") {
			t.Errorf("Expected missing header error, got: %s", responseBody)
		}

		t.Logf("400 response: %s", responseBody)
	})

	// Test directory listing
	t.Run("DirectGETDirectory", func(t *testing.T) {
		// Create HTTP request for root directory
		req := httptest.NewRequest(http.MethodGet, "/api/files/", nil)
		req.Header.Set("X-ClusterF-Internal", "1")

		w := httptest.NewRecorder()

		// Call handleFileGetInternal directly
		cluster.handleFileGetInternal(w, req, "/")

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Check content type
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Expected Content-Type application/json, got %q", ct)
		}

		responseBody := w.Body.String()
		t.Logf("Directory listing response: %s", responseBody)

		// Note: After DELETE, our test file shouldn't be in the listing
		// But we can still test that the directory listing works
		if !strings.Contains(responseBody, "path") || !strings.Contains(responseBody, "entries") {
			t.Errorf("Expected directory listing format with 'path' and 'entries', got: %s", responseBody)
		}
	})

	// Test direct POST operation (directory creation)
	t.Run("DirectPOSTCreateDirectory", func(t *testing.T) {
		dirPath := "/test-directory"

		// Create HTTP request for directory creation
		req := httptest.NewRequest(http.MethodPost, "/api/files"+dirPath, nil)
		req.Header.Set("X-Create-Directory", "true")

		w := httptest.NewRecorder()

		// Call handleFilePost directly
		cluster.handleFilePost(w, req, dirPath)

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Check response body contains success
		responseBody := w.Body.String()
		if !strings.Contains(responseBody, "success") || !strings.Contains(responseBody, "true") {
			t.Errorf("Expected success response, got: %s", responseBody)
		}

		t.Logf("POST directory creation response: %s", responseBody)
	})

	// Test POST without directory header should fail
	t.Run("DirectPOSTWithoutDirectoryHeader", func(t *testing.T) {
		// Create HTTP request without X-Create-Directory header
		req := httptest.NewRequest(http.MethodPost, "/api/files/invalid-post", nil)
		// Missing: X-Create-Directory header

		w := httptest.NewRecorder()

		// Call handleFilePost directly
		cluster.handleFilePost(w, req, "/invalid-post")

		// Check response
		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected status 400, got %d. Response: %s", w.Code, w.Body.String())
		}

		responseBody := w.Body.String()
		if !strings.Contains(responseBody, "Unsupported POST operation") {
			t.Errorf("Expected unsupported operation error, got: %s", responseBody)
		}

		t.Logf("POST without directory header response: %s", responseBody)
	})

	// Test HEAD on directory
	t.Run("DirectHEADDirectory", func(t *testing.T) {
		// Create HTTP request for directory HEAD
		req := httptest.NewRequest(http.MethodHead, "/api/files/", nil)

		w := httptest.NewRecorder()

		// Call handleFileHead directly
		cluster.handleFileHead(w, req, "/")

		// Check response
		if w.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Check headers for directory
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("Expected Content-Type application/json for directory, got %q", ct)
		}

		if isDir := w.Header().Get("X-ClusterF-Is-Directory"); isDir != "true" {
			t.Errorf("Expected X-ClusterF-Is-Directory to be 'true' for directory, got %q", isDir)
		}

		// HEAD should have no body
		if w.Body.Len() != 0 {
			t.Errorf("HEAD response should have no body, got %d bytes", w.Body.Len())
		}

		t.Logf("HEAD directory response headers: %v", w.Header())
	})
}

// TestFileAPI_DirectMultipleFiles tests multiple file operations
func TestFileAPI_DirectMultipleFiles(t *testing.T) {
	testenv.RequireUDPSupport(t)

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "multi-test",
		DataDir:       tempDir,
		DiscoveryPort: 29002,
		Debug:         true,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for the cluster to be ready
	WaitForConditionT(t, "Cluster initialization", func() bool {
		return cluster.FileSystem != nil && cluster.PartitionManager() != nil
	}, 100, 5000)

	// Store multiple files
	files := map[string][]byte{
		"/file1.txt":        []byte("Content of file 1"),
		"/file2.txt":        []byte("Content of file 2"),
		"/subdir/file3.txt": []byte("Content of file 3"),
	}

	for path, content := range files {
		t.Run(fmt.Sprintf("PUT_%s", strings.ReplaceAll(path, "/", "_")), func(t *testing.T) {
			body := bytes.NewReader(content)

			req := httptest.NewRequest(http.MethodPut, "/api/files"+path, body)
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-ClusterF-Modified-At", time.Now().Format(time.RFC3339))

			w := httptest.NewRecorder()
			cluster.handleFilePut(w, req, path)

			if w.Code != http.StatusCreated {
				t.Fatalf("Failed to store %s: status %d, response: %s", path, w.Code, w.Body.String())
			}

			// Rescan partition to register file metadata in CRDT
			partitionID := cluster.PartitionManager().CalculatePartitionName(path)
			t.Logf("Rescanning partition %s for file %s", partitionID, path)
			// Use the correct method name - updatePartitionMetadata is private, so trigger reindex instead
			cluster.PartitionManager().MarkForReindex(types.PartitionID(partitionID))
			cluster.PartitionManager().RunReindex(context.Background())
			t.Logf("Partition rescan completed for %s", path)
		})
	}

	// Retrieve all files
	for path, expectedContent := range files {
		t.Run(fmt.Sprintf("GET_%s", strings.ReplaceAll(path, "/", "_")), func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/files"+path, nil)
			req.Header.Set("X-ClusterF-Internal", "1")

			w := httptest.NewRecorder()
			cluster.handleFileGetInternal(w, req, path)

			if w.Code != http.StatusOK {
				t.Fatalf("Failed to retrieve %s: status %d, response: %s", path, w.Code, w.Body.String())
			}

			responseData := w.Body.Bytes()
			if !bytes.Equal(responseData, expectedContent) {
				t.Fatalf("Content mismatch for %s. Expected %q, got %q", path, expectedContent, responseData)
			}
		})
	}
}

// TestFileAPI_DirectErrorCases tests various error conditions
func TestFileAPI_DirectErrorCases(t *testing.T) {
	testenv.RequireUDPSupport(t)

	tempDir := t.TempDir()

	cluster := NewCluster(ClusterOpts{
		ID:            "error-test",
		DataDir:       tempDir,
		DiscoveryPort: 29003,
		Debug:         true,
	})

	cluster.Start()
	defer cluster.Stop()

	// Wait for the cluster to be ready
	WaitForConditionT(t, "Cluster initialization", func() bool {
		return cluster.FileSystem != nil && cluster.PartitionManager() != nil
	}, 100, 5000)

	t.Run("InvalidTimestamp", func(t *testing.T) {
		body := bytes.NewReader([]byte("test"))

		req := httptest.NewRequest(http.MethodPut, "/api/files/invalid-time.txt", body)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-ClusterF-Modified-At", "invalid-timestamp")

		w := httptest.NewRecorder()
		cluster.handleFilePut(w, req, "/invalid-time.txt")

		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected status 400, got %d. Response: %s", w.Code, w.Body.String())
		}

		if !strings.Contains(w.Body.String(), "Invalid X-ClusterF-Modified-At") {
			t.Errorf("Expected timestamp error, got: %s", w.Body.String())
		}
	})

	t.Run("EmptyFile", func(t *testing.T) {
		body := bytes.NewReader([]byte{})

		req := httptest.NewRequest(http.MethodPut, "/api/files/empty.txt", body)
		req.Header.Set("X-ClusterF-Modified-At", time.Now().Format(time.RFC3339))
		// No Content-Type header for empty file

		w := httptest.NewRecorder()
		cluster.handleFilePut(w, req, "/empty.txt")

		if w.Code != http.StatusCreated {
			t.Fatalf("Expected status 201 for empty file, got %d. Response: %s", w.Code, w.Body.String())
		}
	})

	// Test PUT with forwarded metadata (simulating peer-to-peer transfer) via internal API
	t.Run("ForwardedFile", func(t *testing.T) {
		testData := []byte("Forwarded file content")
		filePath := "/forwarded-file.txt"

		// Create metadata
		metadata := types.FileMetadata{
			Path:        filePath,
			Name:        "forwarded-file.txt",
			Size:        int64(len(testData)),
			ContentType: "text/plain",
			ModifiedAt:  time.Now(),
			CreatedAt:   time.Now(),
			Checksum:    "dummy-checksum",
		}
		metadataJSON, _ := json.Marshal(metadata)
		metadataB64 := base64.StdEncoding.EncodeToString(metadataJSON)

		body := bytes.NewReader(testData)
		req := httptest.NewRequest(http.MethodPut, "/internal/files"+filePath, body)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-Forwarded-From", "peer-node-123")
		req.Header.Set("X-ClusterF-Metadata", metadataB64)

		w := httptest.NewRecorder()
		// Use internal handler which supports forwarded metadata
		cluster.handleFilePutInternal(w, req, filePath)

		if w.Code != http.StatusCreated {
			t.Fatalf("Expected status 201 for forwarded file, got %d. Response: %s", w.Code, w.Body.String())
		}

		// Trigger reindex
		partitionID := cluster.PartitionManager().CalculatePartitionName(filePath)
		cluster.PartitionManager().MarkForReindex(types.PartitionID(partitionID))
		cluster.PartitionManager().RunReindex(context.Background())

		t.Logf("Forwarded file PUT response: %s", w.Body.String())
	})

	// Test PUT with invalid forwarded metadata via internal API
	t.Run("InvalidForwardedMetadata", func(t *testing.T) {
		body := bytes.NewReader([]byte("test"))

		req := httptest.NewRequest(http.MethodPut, "/internal/files/invalid-meta.txt", body)
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-Forwarded-From", "peer-node-123")
		req.Header.Set("X-ClusterF-Metadata", "invalid-base64-metadata")

		w := httptest.NewRecorder()
		// Use internal handler which supports forwarded metadata
		cluster.handleFilePutInternal(w, req, "/invalid-meta.txt")

		if w.Code != http.StatusBadRequest {
			t.Fatalf("Expected status 400 for invalid metadata, got %d. Response: %s", w.Code, w.Body.String())
		}

		if !strings.Contains(w.Body.String(), "Invalid forwarded metadata") {
			t.Errorf("Expected invalid metadata error, got: %s", w.Body.String())
		}
	})

	// Test DELETE on non-existent file
	t.Run("DeleteNonExistentFile", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/files/does-not-exist.txt", nil)

		w := httptest.NewRecorder()
		cluster.handleFileDelete(w, req, "/does-not-exist.txt")

		// DELETE on non-existent file should still return 404
		if w.Code != http.StatusNotFound {
			t.Fatalf("Expected status 404 for non-existent file DELETE, got %d. Response: %s", w.Code, w.Body.String())
		}

		if !strings.Contains(w.Body.String(), "File not found") {
			t.Errorf("Expected file not found error, got: %s", w.Body.String())
		}
	})
}
