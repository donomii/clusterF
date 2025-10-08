package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func assertLastModifiedHeader(t *testing.T, w *httptest.ResponseRecorder, expectedTime time.Time) {
	t.Helper()
	lastModified := w.Header().Get("X-ClusterF-Modified-At")
	if lastModified == "" {
		t.Errorf("X-ClusterF-Modified-At header not set")
		return
	}
	parsedTime, err := time.Parse(time.RFC3339, lastModified)
	if err != nil {
		t.Errorf("Failed to parse X-ClusterF-Modified-At header: %v", err)
		return
	}
	if !parsedTime.Equal(expectedTime) {
		t.Errorf("X-ClusterF-Modified-At time mismatch: got %v, want %v", parsedTime, expectedTime)
	}
}

func TestHandleFilePutModifiedAtHeader(t *testing.T) {
	tempDir := t.TempDir()
	cluster := NewCluster(ClusterOpts{
		ID:            "test-put-modtime",
		DataDir:       filepath.Join(tempDir, "test-put"),
		DiscoveryPort: 29000,
	})
	cluster.Start()
	defer cluster.Stop()

	testCases := []struct {
		name           string
		modifiedAt     string
		expectError    bool
		errorSubstring string
	}{
		{
			name:        "RFC3339Nano with timezone",
			modifiedAt:  "2024-04-27T00:50:34.090000033+09:00",
			expectError: false,
		},
		{
			name:        "RFC3339 with timezone",
			modifiedAt:  "2024-04-27T00:50:34+09:00",
			expectError: false,
		},
		{
			name:        "RFC3339Nano UTC",
			modifiedAt:  "2024-04-27T00:50:34.090000033Z",
			expectError: false,
		},
		{
			name:        "Unix timestamp nanoseconds",
			modifiedAt:  "1714146634090000033",
			expectError: false,
		},
		{
			name:        "Unix timestamp seconds",
			modifiedAt:  "1714146634",
			expectError: false,
		},
		{
			name:           "Missing header",
			modifiedAt:     "",
			expectError:    true,
			errorSubstring: "Missing X-ClusterF-Modified-At",
		},
		{
			name:           "Invalid format",
			modifiedAt:     "not-a-timestamp",
			expectError:    true,
			errorSubstring: "Invalid X-ClusterF-Modified-At",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Sanitize name for use in path
			safeName := tc.name
			safeName = strings.ReplaceAll(safeName, " ", "_")
			safeName = strings.ReplaceAll(safeName, "/", "_")
			path := "/api/files/test_" + safeName + ".txt"
			content := []byte("test content")

			req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(content))
			req.Header.Set("Content-Type", "text/plain")
			if tc.modifiedAt != "" {
				req.Header.Set("X-ClusterF-Modified-At", tc.modifiedAt)
			}

			w := httptest.NewRecorder()
			cluster.handleFilesAPI(w, req)

			if tc.expectError {
				if w.Code == http.StatusCreated {
					t.Errorf("Expected error but got success")
				}
			} else {
				if w.Code != http.StatusCreated {
					t.Errorf("Expected 201 Created, got %d: %s", w.Code, w.Body.String())
				}
			}
		})
	}
}

func TestHandleFileGetModifiedAtHeader(t *testing.T) {
	tempDir := t.TempDir()
	cluster := NewCluster(ClusterOpts{
		ID:            "test-get-modtime",
		DataDir:       filepath.Join(tempDir, "test-get"),
		DiscoveryPort: 29001,
	})
	cluster.Start()
	defer cluster.Stop()

	path := "/api/files/test_get_modtime.txt"
	content := []byte("test content")
	modTimeStr := "2024-04-27T00:50:34+09:00"
	expectedTime, err := time.Parse(time.RFC3339, modTimeStr)
	if err != nil {
		t.Fatalf("Failed to parse test time: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", modTimeStr)

	w := httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to store file: %d %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, path, nil)
	w = httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Failed to retrieve file: %d %s", w.Code, w.Body.String())
	}

	modifiedAt := w.Header().Get("X-ClusterF-Modified-At")
	if modifiedAt == "" {
		t.Errorf("X-ClusterF-Modified-At header not set")
	} else {
		parsedTime, err := time.Parse(time.RFC3339, modifiedAt)
		if err != nil {
			t.Errorf("Failed to parse X-ClusterF-Modified-At header: %v", err)
		} else {
			if !parsedTime.Equal(expectedTime) {
				t.Errorf("X-ClusterF-Modified-At time mismatch: got %v, want %v", parsedTime, expectedTime)
			}
		}
	}

	assertLastModifiedHeader(t, w, expectedTime)

	createdAt := w.Header().Get("X-ClusterF-Created-At")
	if createdAt == "" {
		t.Errorf("X-ClusterF-Created-At header not set")
	} else {
		parsedTime, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			t.Errorf("Failed to parse X-ClusterF-Created-At header: %v", err)
		} else {
			if !parsedTime.Equal(expectedTime) {
				t.Errorf("X-ClusterF-Created-At time mismatch: got %v, want %v", parsedTime, expectedTime)
			}
		}
	}
}

func TestHandleFileHeadModifiedAtHeader(t *testing.T) {
	tempDir := t.TempDir()
	cluster := NewCluster(ClusterOpts{
		ID:            "test-head-modtime",
		DataDir:       filepath.Join(tempDir, "test-head"),
		DiscoveryPort: 29002,
	})
	cluster.Start()
	defer cluster.Stop()

	path := "/api/files/test_head_modtime.txt"
	content := []byte("test content")
	modTimeStr := "2024-04-27T00:50:34+09:00"
	expectedTime, err := time.Parse(time.RFC3339, modTimeStr)
	if err != nil {
		t.Fatalf("Failed to parse test time: %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(content))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", modTimeStr)

	w := httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed to store file: %d %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodHead, path, nil)
	w = httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Failed HEAD request: %d %s", w.Code, w.Body.String())
	}

	assertLastModifiedHeader(t, w, expectedTime)

	createdAt := w.Header().Get("X-ClusterF-Created-At")
	if createdAt == "" {
		t.Errorf("X-ClusterF-Created-At header not set")
	} else {
		parsedTime, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			t.Errorf("Failed to parse X-ClusterF-Created-At header: %v", err)
		} else {
			if !parsedTime.Equal(expectedTime) {
				t.Errorf("X-ClusterF-Created-At time mismatch: got %v, want %v", parsedTime, expectedTime)
			}
		}
	}

	body, _ := io.ReadAll(w.Body)
	if len(body) > 0 {
		t.Errorf("HEAD request returned body content: %d bytes", len(body))
	}
}

/*
func TestHandleFileTimezonePreservation(t *testing.T) {
	tempDir := t.TempDir()
	cluster := NewCluster(ClusterOpts{
		ID:            "test-tz-preservation",
		DataDir:       filepath.Join(tempDir, "test-tz"),
		DiscoveryPort: 29003,
	})
	cluster.Start()
	defer cluster.Stop()

	testCases := []struct {
		name       string
		modifiedAt string
	}{
		{
			name:       "JST_timezone_+0900",
			modifiedAt: "2024-04-27T00:50:34+09:00",
		},
		{
			name:       "EST_timezone_-0500",
			modifiedAt: "2024-04-27T00:50:34-05:00",
		},
		{
			name:       "UTC_timezone",
			modifiedAt: "2024-04-27T00:50:34Z",
		},
		{
			name:       "PST_timezone_-0800",
			modifiedAt: "2024-04-27T00:50:34-08:00",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Sanitize name for use in path
			safeName := tc.name
			safeName = strings.ReplaceAll(safeName, " ", "_")
			safeName = strings.ReplaceAll(safeName, "/", "_")
			path := "/api/files/test_tz_" + safeName + ".txt"
			content := []byte("test content")
			expectedTime, err := time.Parse(time.RFC3339, tc.modifiedAt)
			if err != nil {
				t.Fatalf("Failed to parse test time: %v", err)
			}

			req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(content))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-ClusterF-Modified-At", tc.modifiedAt)

			w := httptest.NewRecorder()
			cluster.handleFilesAPI(w, req)

			if w.Code != http.StatusCreated {
				t.Fatalf("Failed to store file: %d %s", w.Code, w.Body.String())
			}

			req = httptest.NewRequest(http.MethodGet, path, nil)
			w = httptest.NewRecorder()
			cluster.handleFilesAPI(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("Failed to retrieve file: %d %s", w.Code, w.Body.String())
			}

			WaitForConditionT(t, "Wait for metadata", func() bool {

				_, err := cluster.FileSystem.GetMetadata(path)
				if err != nil {
					return false
				}
				return true
			}, 200, 10000)

			metadata, err := cluster.FileSystem.GetMetadata(path)
			if err != nil {
				t.Fatalf("Failed to get metadata: %v", err)
			}

			if !metadata.ModifiedAt.Equal(expectedTime) {
				t.Errorf("ModifiedAt mismatch: got %v (%s), want %v (%s)",
					metadata.ModifiedAt, metadata.ModifiedAt.Location(),
					expectedTime, expectedTime.Location())
			}

			if !metadata.CreatedAt.Equal(expectedTime) {
				t.Errorf("CreatedAt mismatch: got %v (%s), want %v (%s)",
					metadata.CreatedAt, metadata.CreatedAt.Location(),
					expectedTime, expectedTime.Location())
			}

			body, _ := io.ReadAll(w.Body)
			if !bytes.Equal(body, content) {
				t.Errorf("Content mismatch: got %q, want %q", body, content)
			}
		})
	}
}
*/

func TestHandleFileUpdatePreservesCreatedAt(t *testing.T) {
	tempDir := t.TempDir()
	cluster := NewCluster(ClusterOpts{
		ID:            "test-update-created",
		DataDir:       filepath.Join(tempDir, "test-update"),
		DiscoveryPort: 29004,
	})
	cluster.Start()
	defer cluster.Stop()

	path := "/api/files/test_update.txt"

	initialTime := "2024-04-27T00:50:34+09:00"
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader([]byte("initial content")))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", initialTime)

	w := httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed initial store: %d %s", w.Code, w.Body.String())
	}

	expectedCreatedTime, err := time.Parse(time.RFC3339, initialTime)
	if err != nil {
		t.Fatalf("Failed to parse initial time: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	updateTime := "2024-04-27T01:00:00+09:00"
	req = httptest.NewRequest(http.MethodPut, path, bytes.NewReader([]byte("updated content")))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-ClusterF-Modified-At", updateTime)

	w = httptest.NewRecorder()
	cluster.handleFilesAPI(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("Failed update: %d %s", w.Code, w.Body.String())
	}

	expectedModifiedTime, err := time.Parse(time.RFC3339, updateTime)
	if err != nil {
		t.Fatalf("Failed to parse update time: %v", err)
	}

	metadata, err := cluster.FileSystem.GetMetadata(path)
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if !metadata.CreatedAt.Equal(expectedCreatedTime) {
		t.Errorf("CreatedAt changed on update: got %v, want %v",
			metadata.CreatedAt, expectedCreatedTime)
	}

	if !metadata.ModifiedAt.Equal(expectedModifiedTime) {
		t.Errorf("ModifiedAt not updated: got %v, want %v",
			metadata.ModifiedAt, expectedModifiedTime)
	}
}
