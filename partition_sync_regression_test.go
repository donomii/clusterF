package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func TestReviewSyncCreatesMissingReplicaAndPropagatesDeletion(t *testing.T) {
	source, _ := newLoopbackCluster(t, "sync-source", false)
	target, _ := newLoopbackCluster(t, "sync-target", false)
	registerLoopbackPeer(t, source, target, 99)
	registerLoopbackPeer(t, target, source, 99)
	path := "/replicate.txt"
	modified := time.Now().Add(-time.Hour)
	storeLoopbackFile(t, source, path, []byte("replicated content"), modified)
	setLoopbackHolders(source, path, source.ID(), target.ID())
	setLoopbackHolders(target, path, source.ID(), target.ID())
	if err := source.partitionManager.SyncFile(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	content, _, err := target.FileSystem.GetFile(path)
	if err != nil || string(content) != "replicated content" {
		t.Fatalf("missing replica content = %q, error %v", content, err)
	}
	deleted := modified.Add(time.Minute)
	if err := source.FileSystem.DeleteFileWithTimestamp(context.Background(), path, deleted); err != nil {
		t.Fatal(err)
	}
	setLoopbackHolders(source, path, source.ID(), target.ID())
	if err := source.partitionManager.SyncFile(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if _, err := target.FileSystem.GetMetadata(path); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("deleted replica metadata error = %v", err)
	}
}

func TestReviewSyncRejectsCorruptReplicaContent(t *testing.T) {
	source, _ := newLoopbackCluster(t, "corrupt-source", false)
	target, _ := newLoopbackCluster(t, "corrupt-target", false)
	registerLoopbackPeer(t, source, target, 99)
	registerLoopbackPeer(t, target, source, 99)
	path := "/corrupt.txt"
	storeLoopbackFile(t, source, path, []byte("good"), time.Now().Add(-time.Hour))
	store := source.partitionManager.FileStore()
	metadata, err := store.GetMetadata(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(path, metadata, []byte("evil")); err != nil {
		t.Fatal(err)
	}
	setLoopbackHolders(source, path, source.ID(), target.ID())
	err = source.partitionManager.SyncFile(context.Background(), path)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "checksum") {
		t.Fatalf("corrupt content sync error = %v", err)
	}
	if _, err := target.FileSystem.GetMetadata(path); !errors.Is(err, types.ErrFileNotFound) {
		t.Fatalf("corrupt content was copied to target: %v", err)
	}
}
