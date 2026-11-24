package partitionmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/donomii/clusterF/metrics"
	"github.com/donomii/clusterF/types"
)

func storageTestMetadata(path string, content []byte) []byte {
	digest := sha256.Sum256(content)
	metadata := types.FileMetadata{Path: path, Size: int64(len(content)), Checksum: fmt.Sprintf("%x", digest), ModifiedAt: time.Now(), ContentType: "application/octet-stream"}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}
	return encoded
}

func verifyStoredContent(metadata, content []byte) error {
	var decoded types.FileMetadata
	if err := json.Unmarshal(metadata, &decoded); err != nil {
		return err
	}
	digest := sha256.Sum256(content)
	if decoded.Size != int64(len(content)) || decoded.Checksum != fmt.Sprintf("%x", digest) {
		return fmt.Errorf("metadata/content mismatch: expected size=%d checksum=%s, received size=%d checksum=%x", decoded.Size, decoded.Checksum, len(content), digest)
	}
	return nil
}

func TestDiskConcurrentStreamSnapshots(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "storage-test"})
	store := NewDiskFileStore(t.TempDir())
	defer store.Close()
	path := "/concurrent.bin"
	initial := []byte("initial")
	if err := store.Put(path, storageTestMetadata(path, initial), initial); err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	failures := make(chan error, 8)
	for writer := 0; writer < 4; writer++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 30; iteration++ {
				content := bytes.Repeat([]byte{byte(writer + 1)}, (writer+1)*2048+iteration)
				if err := store.PutStream(path, storageTestMetadata(path, content), bytes.NewReader(content), int64(len(content))); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	for reader := 0; reader < 4; reader++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				metadata, stream, err := store.GetStream(path)
				if err != nil {
					failures <- err
					return
				}
				content, err := io.ReadAll(stream)
				stream.Close()
				if err == nil {
					err = verifyStoredContent(metadata, content)
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestDiskPublicationFailurePreservesContent(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "storage-test"})
	store := NewDiskFileStore(t.TempDir())
	defer store.Close()
	contentPath := filepath.Join(store.baseDir, "old-content")
	metadataPath := filepath.Join(store.baseDir, "unreplaceable-directory")
	contentTemp := filepath.Join(store.transferDir, "new-content")
	metadataTemp := filepath.Join(store.transferDir, "new-metadata")
	for path, data := range map[string]string{contentPath: "old bytes", contentTemp: "new bytes", metadataTemp: "new metadata"} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(metadataPath, 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.publishPair(contentTemp, metadataTemp, contentPath, metadataPath); err == nil {
		t.Fatal("publication unexpectedly succeeded with directory as metadata destination")
	}
	content, err := os.ReadFile(contentPath)
	if err != nil || string(content) != "old bytes" {
		t.Fatalf("previous content was not restored: %q, %v", content, err)
	}
}

func TestStoreEncryptionRoundTrip(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "storage-test"})
	for _, backend := range []string{"rawfile", "bolt"} {
		t.Run(backend, func(t *testing.T) {
			var store types.FileStoreLike
			if backend == "rawfile" {
				store = NewDiskFileStore(t.TempDir())
			} else {
				store = NewFileStore(t.TempDir(), false, "bolt", "")
			}
			defer store.Close()
			key := bytes.Repeat([]byte{5}, 32)
			store.SetEncryptionKey(key)
			path := "/encrypted.bin"
			content := bytes.Repeat([]byte("secret contents"), 10000)
			metadata := storageTestMetadata(path, content)
			if err := store.Put(path, metadata, content); err != nil {
				t.Fatal(err)
			}
			gotMetadata, gotContent, exists, err := store.Get(path)
			if err != nil || !exists || !bytes.Equal(gotMetadata, metadata) || !bytes.Equal(gotContent, content) {
				t.Fatalf("encrypted round trip failed: exists=%t metadata=%t content=%t error=%v", exists, bytes.Equal(gotMetadata, metadata), bytes.Equal(gotContent, content), err)
			}
			store.SetEncryptionKey(bytes.Repeat([]byte{6}, 32))
			if _, _, _, err := store.Get(path); err == nil {
				t.Fatal("wrong encryption key accepted")
			}
		})
	}
}

func TestVerifiedDownloadRejectsChangedContent(t *testing.T) {
	content := []byte("downloaded bytes")
	digest := sha256.Sum256(content)
	if _, _, err := copyVerifiedFile(bytes.NewReader(content), "/file", "different checksum"); err == nil {
		t.Fatal("unverified content accepted")
	}
	path, size, err := copyVerifiedFile(bytes.NewReader(content), "/file", fmt.Sprintf("%x", digest))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	stored, err := os.ReadFile(path)
	if err != nil || size != int64(len(content)) || !bytes.Equal(stored, content) {
		t.Fatalf("verified download mismatch: size=%d content=%q error=%v", size, stored, err)
	}
}

func TestDeletionWinsEqualTimestamp(t *testing.T) {
	modified := time.Now()
	live := types.FileMetadata{Path: "/file", ModifiedAt: modified, Checksum: "fff"}
	deleted := types.FileMetadata{Path: "/file", ModifiedAt: modified, Deleted: true}
	if !metadataIsNewer(deleted, live) || metadataIsNewer(live, deleted) {
		t.Fatal("equal-timestamp deletion did not consistently win")
	}
}

func TestKVScansUseStoredKeyPrefixes(t *testing.T) {
	metrics.NewMetricsCollector(&types.App{NodeID: "storage-test"})
	for _, backend := range []string{"bolt", "extent"} {
		t.Run(backend, func(t *testing.T) {
			store := NewFileStore(t.TempDir(), false, backend, "")
			defer store.Close()
			path := "/scan-prefix/file.txt"
			partition := types.PartitionIDForPath(path)
			storeID := types.ExtractPartitionStoreID(partition)
			neighbor := ""
			for candidate := 0; candidate < 100000; candidate++ {
				candidatePath := fmt.Sprintf("/other-prefix/%d.txt", candidate)
				candidatePartition := types.PartitionIDForPath(candidatePath)
				if candidatePartition != partition && types.ExtractPartitionStoreID(candidatePartition) == storeID {
					neighbor = candidatePath
					break
				}
			}
			if neighbor == "" {
				t.Fatal("no neighboring partition found")
			}
			for _, filePath := range []string{path, neighbor} {
				content := []byte(filePath)
				if err := store.Put(filePath, storageTestMetadata(filePath, content), content); err != nil {
					t.Fatal(err)
				}
			}
			var partitionPaths []string
			err := store.ScanMetadataPartition(context.Background(), partition, func(filePath string, metadata []byte) error {
				partitionPaths = append(partitionPaths, filePath)
				return nil
			})
			if err != nil || len(partitionPaths) != 1 || partitionPaths[0] != path {
				t.Fatalf("partition %s scan returned %v, error=%v", partition, partitionPaths, err)
			}
			var metadataPaths []string
			err = store.ScanMetadata("/scan-prefix/", func(filePath string, metadata []byte) error {
				metadataPaths = append(metadataPaths, filePath)
				return nil
			})
			if err != nil || len(metadataPaths) != 1 || metadataPaths[0] != path {
				t.Fatalf("metadata prefix scan returned %v, error=%v", metadataPaths, err)
			}
			var contentPaths []string
			err = store.Scan("/scan-prefix/", func(filePath string, metadata, content []byte) error {
				contentPaths = append(contentPaths, filePath)
				return verifyStoredContent(metadata, content)
			})
			if err != nil || len(contentPaths) != 1 || contentPaths[0] != path {
				t.Fatalf("content prefix scan returned %v, error=%v", contentPaths, err)
			}
		})
	}
}
