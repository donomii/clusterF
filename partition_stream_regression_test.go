package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/types"
)

func TestReviewPartitionStreamAppliesTombstone(t *testing.T) {
	source, _ := newLoopbackCluster(t, "stream-delete-source", false)
	target, _ := newLoopbackCluster(t, "stream-delete-target", false)
	registerLoopbackPeer(t, target, source, 99)
	path := "/stream-delete.txt"
	modified := time.Now().Add(-time.Hour)
	storeLoopbackFile(t, target, path, []byte("old bytes"), modified)
	metadata := types.FileMetadata{Path: path, ModifiedAt: modified.Add(time.Minute), DeletedAt: modified.Add(time.Minute), Deleted: true}
	response := applyPartitionStream(t, target, source, metadata)
	if response.Code != http.StatusOK {
		t.Fatalf("tombstone stream returned %d: %s", response.Code, response.Body.String())
	}
	if _, err := target.FileSystem.GetMetadata(path); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("deleted file remained readable: %v", err)
	}
	encoded, err := target.partitionManager.FileStore().GetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	var stored types.FileMetadata
	if err := json.Unmarshal(encoded, &stored); err != nil || !stored.Deleted {
		t.Fatalf("tombstone was not retained: %s, %v", encoded, err)
	}
	partition := types.PartitionIDForPath(path)
	target.partitionManager.MarkForReindex(partition, "verify tombstone membership")
	target.partitionManager.RunReindex(context.Background())
	holders := target.GetPartitionHolders(partition)
	if len(holders) != 1 || holders[0] != target.ID() {
		t.Fatalf("tombstone partition lost holder after reindex: %v", holders)
	}
}

func TestReviewReindexPreservesMembershipOnInvalidMetadata(t *testing.T) {
	cluster, _ := newLoopbackCluster(t, "reindex-failure", false)
	path := "/reindex-failure.txt"
	storeLoopbackFile(t, cluster, path, []byte("content"), time.Now().Add(-time.Hour))
	partition := types.PartitionIDForPath(path)
	cluster.partitionManager.MarkForReindex(partition, "verify initial membership")
	cluster.partitionManager.RunReindex(context.Background())
	if err := cluster.partitionManager.FileStore().PutMetadata(path, []byte("invalid metadata")); err != nil {
		t.Fatal(err)
	}
	cluster.partitionManager.MarkForReindex(partition, "verify failed reindex retains membership")
	cluster.partitionManager.RunReindex(context.Background())
	holders := cluster.GetPartitionHolders(partition)
	if len(holders) != 1 || holders[0] != cluster.ID() {
		t.Fatalf("failed reindex removed partition holder: %v", holders)
	}
	if pending, _ := cluster.partitionManager.ReindexList.Load(partition); !pending {
		t.Fatal("failed reindex was not queued for retry")
	}
}

func TestReviewPartitionStreamCreatesLocalReplicaAndRejectsCorruption(t *testing.T) {
	source, _ := newLoopbackCluster(t, "stream-file-source", false)
	target, _ := newLoopbackCluster(t, "stream-file-target", false)
	registerLoopbackPeer(t, target, source, 99)
	path := "/stream-file.txt"
	storeLoopbackFile(t, source, path, []byte("good bytes"), time.Now().Add(-time.Hour))
	metadata, err := source.FileSystem.GetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	response := applyPartitionStream(t, target, source, metadata)
	if response.Code != http.StatusOK {
		t.Fatalf("file stream returned %d: %s", response.Code, response.Body.String())
	}
	content, _, err := target.FileSystem.GetFile(path)
	if err != nil || string(content) != "good bytes" {
		t.Fatalf("local replica = %q, error=%v", content, err)
	}
	metadata.ModifiedAt = metadata.ModifiedAt.Add(time.Minute)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.partitionManager.FileStore().Put(path, encoded, []byte("evil bytes")); err != nil {
		t.Fatal(err)
	}
	response = applyPartitionStream(t, target, source, metadata)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("corrupt file stream returned %d: %s", response.Code, response.Body.String())
	}
	content, _, err = target.FileSystem.GetFile(path)
	if err != nil || string(content) != "good bytes" {
		t.Fatalf("failed synchronization changed existing replica: %q, %v", content, err)
	}
}

func applyPartitionStream(t *testing.T, target, source *Cluster, metadata types.FileMetadata) *httptest.ResponseRecorder {
	t.Helper()
	encoded, err := json.Marshal(partitionmanager.PartitionSyncEntry{Path: metadata.Path, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/partition-sync/"+string(types.PartitionIDForPath(metadata.Path)), bytes.NewReader(encoded))
	request.Header.Set("X-ClusterF-Node", string(source.ID()))
	response := httptest.NewRecorder()
	target.partitionManager.HandlePartitionSync(response, request, types.PartitionIDForPath(metadata.Path))
	return response
}
