package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestStatusHandlerReturnsJSON(t *testing.T) {
	cluster := NewCluster(ClusterOpts{
		ID:           "status-node",
		DataDir:      filepath.Join(t.TempDir(), "data"),
		HTTPDataPort: 4321,
	})
	defer cluster.Stop()

	if err := cluster.FileSystem.StoreFile("/test.txt", []byte("hello"), "text/plain"); err != nil {
		t.Fatalf("store file failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rr := httptest.NewRecorder()
	cluster.handleStatus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rr.Code)
	}

	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse status json: %v", err)
	}
	t.Logf("status response: %s", rr.Body.Bytes())

	partitionStats, ok := body["partition_stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing partition_stats in response: %v", body)
	}

	if totalFiles, ok := partitionStats["total_files"].(float64); !ok || totalFiles < 1 {
		t.Fatalf("expected total_files >= 1, got %v", partitionStats["total_files"])
	}
}
