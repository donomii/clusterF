// indexer_test.go - Tests for the file indexer
package indexer

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/donomii/clusterF/partitionmanager"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/frogpond"
)

type testCluster struct {
	id     types.NodeID
	logger *log.Logger
	pm     types.PartitionManagerLike
}

func (c *testCluster) PartitionManager() types.PartitionManagerLike { return c.pm }
func (c *testCluster) DiscoveryManager() types.DiscoveryManagerLike { return nil }
func (c *testCluster) Exporter() types.ExporterLike                 { return nil }
func (c *testCluster) Logger() *log.Logger                          { return c.logger }
func (c *testCluster) ReplicationFactor() int                       { return 0 }
func (c *testCluster) NoStore() bool                                { return true }
func (c *testCluster) ListDirectoryUsingSearch(string) ([]*types.FileMetadata, error) {
	return nil, nil
}
func (c *testCluster) DataClient() *http.Client                      { return nil }
func (c *testCluster) ID() types.NodeID                              { return c.id }
func (c *testCluster) GetAllNodes() map[types.NodeID]*types.NodeData { return nil }
func (c *testCluster) GetNodesForPartition(string) []types.NodeID    { return nil }
func (c *testCluster) GetNodeInfo(types.NodeID) *types.NodeData      { return nil }
func (c *testCluster) GetPartitionSyncPaused() bool                  { return false }
func (c *testCluster) AppContext() context.Context                   { return context.Background() }
func (c *testCluster) RecordDiskActivity(types.DiskActivityLevel)    {}
func (c *testCluster) CanRunNonEssentialDiskOp() bool                { return true }
func (c *testCluster) LoadPeer(types.NodeID) (*types.PeerInfo, bool) { return nil, false }

type testFileStore struct {
	meta map[string][]byte
}

func (f *testFileStore) Close()                                   {}
func (f *testFileStore) SetEncryptionKey([]byte)                  {}
func (f *testFileStore) Get(string) ([]byte, []byte, bool, error) { return nil, nil, false, nil }
func (f *testFileStore) GetMetadata(path string) ([]byte, error)  { return f.meta[path], nil }
func (f *testFileStore) GetContent(string) ([]byte, error)        { return nil, nil }
func (f *testFileStore) Put(string, []byte, []byte) error         { return nil }
func (f *testFileStore) PutMetadata(path string, data []byte) error {
	if f.meta == nil {
		f.meta = make(map[string][]byte)
	}
	f.meta[path] = data
	return nil
}
func (f *testFileStore) putMeta(meta types.FileMetadata) {
	if f.meta == nil {
		f.meta = make(map[string][]byte)
	}
	encoded, _ := json.Marshal(meta)
	f.meta[meta.Path] = encoded
}
func (f *testFileStore) Delete(string) error                                   { return nil }
func (f *testFileStore) Scan(string, func(string, []byte, []byte) error) error { return nil }
func (f *testFileStore) ScanMetadata(string, func(string, []byte) error) error { return nil }
func (f *testFileStore) ScanMetadataPartition(context.Context, types.PartitionID, func(string, []byte) error) error {
	return nil
}
func (f *testFileStore) CalculatePartitionChecksum(context.Context, types.PartitionID) (string, error) {
	return "checksum", nil
}
func (f *testFileStore) GetAllPartitionStores() ([]types.PartitionStore, error) { return nil, nil }

func newTestApp(logger *log.Logger) *types.App {
	app := &types.App{
		NodeID:             "test-node",
		NoStore:            true,
		Logger:             logger,
		FileStore:          &testFileStore{meta: make(map[string][]byte)},
		Frogpond:           frogpond.NewNode(),
		SendUpdatesToPeers: func([]frogpond.DataPoint) {},
	}

	cluster := &testCluster{id: "test-node", logger: logger}
	app.Cluster = cluster
	cluster.pm = partitionmanager.NewPartitionManager(app)

	return app
}

func newTestIndexer(logger *log.Logger, indexType IndexType) *Indexer {
	return NewIndexerWithType(logger, indexType, newTestApp(logger))
}

func containsPath(paths []string, target string) bool {
	for _, p := range paths {
		if p == target {
			return true
		}
	}
	return false
}

func TestIndexerBasics(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, IndexTypeTrie)
	fs := idx.deps.FileStore.(*testFileStore)

	// Test adding files
	metadata1 := types.FileMetadata{
		Name:        "test1.txt",
		Path:        "/docs/test1.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "abc123",
	}
	fs.putMeta(metadata1)
	idx.AddFile("/docs/test1.txt", metadata1)

	metadata2 := types.FileMetadata{
		Name:        "test2.txt",
		Path:        "/docs/test2.txt",
		Size:        200,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "def456",
	}
	fs.putMeta(metadata2)
	idx.AddFile("/docs/test2.txt", metadata2)

	metadata3 := types.FileMetadata{
		Name:        "other.txt",
		Path:        "/other/other.txt",
		Size:        300,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "ghi789",
	}
	fs.putMeta(metadata3)
	idx.AddFile("/other/other.txt", metadata3)

	for _, path := range []string{"/docs/test1.txt", "/docs/test2.txt", "/other/other.txt"} {
		partitionID := types.PartitionIDForPath(path)
		partitionPaths := idx.FilesForPartition(partitionID)
		if !containsPath(partitionPaths, path) {
			t.Errorf("Expected partition %s to contain %s", partitionID, path)
		}
	}

	// Test prefix search
	results := idx.PrefixSearch("/docs/")
	if len(results) != 2 {
		t.Errorf("Expected 2 results for /docs/ prefix, got %d", len(results))
	}

	results = idx.PrefixSearch("/other/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for /other/ prefix, got %d", len(results))
	}

	results = idx.PrefixSearch("/")
	if len(results) != 2 {
		t.Errorf("Expected 2 results for / prefix, got %d", len(results))
	}

	// Test delete
	idx.DeleteFile("/docs/test1.txt")
	results = idx.PrefixSearch("/docs/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result after delete, got %d", len(results))
	}

	deletedPartition := types.PartitionIDForPath("/docs/test1.txt")
	if !containsPath(idx.FilesForPartition(deletedPartition), "/docs/test1.txt") {
		t.Errorf("Expected partition %s to retain tombstone for /docs/test1.txt", deletedPartition)
	}
}

func TestIndexerUpdate(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, IndexTypeTrie)
	fs := idx.deps.FileStore.(*testFileStore)

	// Add a file
	metadata := types.FileMetadata{
		Name:        "test.txt",
		Path:        "/test.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "abc123",
	}
	fs.putMeta(metadata)
	idx.AddFile("/test.txt", metadata)

	// Update the same file
	updatedMetadata := types.FileMetadata{
		Name:        "test.txt",
		Path:        "/test.txt",
		Size:        200,
		ContentType: "text/plain",
		ModifiedAt:  time.Now().Add(time.Hour),
		Checksum:    "xyz789",
	}
	fs.putMeta(updatedMetadata)
	idx.AddFile("/test.txt", updatedMetadata)

	// Should still have only one result
	results := idx.PrefixSearch("/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result after update, got %d", len(results))
	}

	// Check that size was updated
	if results[0].Size != 200 {
		t.Errorf("Expected size 200 after update, got %d", results[0].Size)
	}
}

func TestIndexerDeletedFiles(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, IndexTypeTrie)
	fs := idx.deps.FileStore.(*testFileStore)

	// Add a file
	metadata := types.FileMetadata{
		Name:        "test.txt",
		Path:        "/test.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "abc123",
		Deleted:     false,
	}
	fs.putMeta(metadata)
	idx.AddFile("/test.txt", metadata)

	// Add a deleted file (should be filtered out in search)
	deletedMetadata := types.FileMetadata{
		Name:        "deleted.txt",
		Path:        "/deleted.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "def456",
		Deleted:     true,
		DeletedAt:   time.Now(),
	}
	fs.putMeta(deletedMetadata)
	idx.AddFile("/deleted.txt", deletedMetadata)

	deletedPartition := types.PartitionIDForPath("/deleted.txt")
	if !containsPath(idx.FilesForPartition(deletedPartition), "/deleted.txt") {
		t.Errorf("Expected partition %s to include /deleted.txt tombstone", deletedPartition)
	}

	// Search should not return deleted files
	results := idx.PrefixSearch("/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result (deleted files filtered), got %d", len(results))
	}

	if results[0].Path != "/test.txt" {
		t.Errorf("Expected /test.txt, got %s", results[0].Path)
	}
}

func BenchmarkPrefixSearch_Trie(b *testing.B) {
	benchmarkPrefixSearch(b, IndexTypeTrie)
}

func benchmarkPrefixSearch(b *testing.B, indexType IndexType) {
	logger := log.New(os.Stdout, "[BENCH] ", log.LstdFlags)
	idx := newTestIndexer(logger, indexType)

	// Add 1000 files
	for i := 0; i < 1000; i++ {
		metadata := types.FileMetadata{
			Name:        "test.txt",
			Path:        "/docs/subdir/test.txt",
			Size:        100,
			ContentType: "text/plain",
			ModifiedAt:  time.Now(),
			Checksum:    "abc123",
		}
		idx.AddFile(metadata.Path, metadata)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.PrefixSearch("/docs/")
	}
}
