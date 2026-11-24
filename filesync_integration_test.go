package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donomii/clusterF/filesync"
	"github.com/donomii/clusterF/types"
)

func awaitLoopbackCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestReviewFileSyncRenameAndRapidEdit(t *testing.T) {
	cluster, _ := newLoopbackCluster(t, "watcher", false)
	cluster.frogpond.SetDataPoint("cluster/replication_factor", []byte("1"))
	directory := t.TempDir()
	oldPath := filepath.Join(directory, "old.txt")
	if err := os.WriteFile(oldPath, []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	oldDirectory := filepath.Join(directory, "old-directory")
	if err := os.Mkdir(oldDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldDirectory, "child.txt"), []byte("directory content"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	syncer, err := filesync.NewFileSyncer(directory, "", "/mirror", "", log.New(io.Discard, "", 0), cluster.FileSystem)
	if err != nil {
		t.Fatal(err)
	}
	syncer.Run(ctx)
	cluster.rebuildPartitionHolderMap(ctx)
	marker := filepath.Join(directory, "ready.txt")
	awaitLoopbackCondition(t, "filesystem watcher readiness", func() bool {
		if err := os.WriteFile(marker, []byte("ready"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := cluster.FileSystem.GetMetadata("/mirror/ready.txt")
		return err == nil
	})
	newPath := filepath.Join(directory, "new.txt")
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	awaitLoopbackCondition(t, "old cluster path deletion after rename", func() bool {
		_, err := cluster.FileSystem.GetMetadata("/mirror/old.txt")
		return errors.Is(err, types.ErrFileNotFound)
	})
	awaitLoopbackCondition(t, "renamed cluster file", func() bool {
		content, _, err := cluster.FileSystem.GetFile("/mirror/new.txt")
		return err == nil && string(content) == "before"
	})
	cluster.rebuildPartitionHolderMap(ctx)
	if err := os.WriteFile(newPath, []byte("after!"), 0600); err != nil {
		t.Fatal(err)
	}
	awaitLoopbackCondition(t, "same-size edit in cluster", func() bool {
		content, _, err := cluster.FileSystem.GetFile("/mirror/new.txt")
		return err == nil && string(content) == "after!"
	})
	if err := os.Rename(oldDirectory, filepath.Join(directory, "new-directory")); err != nil {
		t.Fatal(err)
	}
	awaitLoopbackCondition(t, "renamed directory contents", func() bool {
		content, _, err := cluster.FileSystem.GetFile("/mirror/new-directory/child.txt")
		return err == nil && string(content) == "directory content"
	})
	awaitLoopbackCondition(t, "old directory contents deletion", func() bool {
		_, err := cluster.FileSystem.GetMetadata("/mirror/old-directory/child.txt")
		return errors.Is(err, types.ErrFileNotFound)
	})
}

func TestReviewImportWaitsForOutstandingUpload(t *testing.T) {
	cluster, _ := newLoopbackCluster(t, "importer", true)
	storage, _ := newLoopbackCluster(t, "import-storage", false)
	registerLoopbackPeer(t, cluster, storage, 99)
	cluster.frogpond.SetDataPoint("cluster/replication_factor", []byte("1"))
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "pending.txt"), []byte("pending upload"), 0600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "import.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	syncer, err := filesync.NewFileSyncer("", directory, "/import", "", log.New(logFile, "", 0), cluster.FileSystem)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	syncer.SetCurrentFileCallback(func(path string) {
		if path != "" {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage.internalCallThrottle <- struct{}{}
	released := false
	defer func() {
		if !released {
			<-storage.internalCallThrottle
		}
	}()
	done := make(chan struct{})
	go func() {
		syncer.Run(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("import did not start")
	}
	time.Sleep(50 * time.Millisecond)
	output, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output), "Import completed successfully") {
		t.Fatal("import reported completion while upload was blocked")
	}
	<-storage.internalCallThrottle
	released = true
	awaitLoopbackCondition(t, "completed import", func() bool {
		output, err := os.ReadFile(logPath)
		return err == nil && strings.Contains(string(output), "Import completed successfully")
	})
	content, _, err := storage.FileSystem.GetFile("/import/pending.txt")
	if err != nil || string(content) != "pending upload" {
		t.Fatalf("imported content = %q, error %v", content, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("import did not finish after cancellation")
	}
}
