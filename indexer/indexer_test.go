// indexer_test.go - Tests for the file indexer
package indexer

import (
	"context"
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

type testFileStore struct{}

func (f *testFileStore) Close()                                                {}
func (f *testFileStore) SetEncryptionKey([]byte)                               {}
func (f *testFileStore) Get(string) ([]byte, []byte, bool, error)              { return nil, nil, false, nil }
func (f *testFileStore) GetMetadata(string) ([]byte, error)                    { return nil, nil }
func (f *testFileStore) GetContent(string) ([]byte, error)                     { return nil, nil }
func (f *testFileStore) Put(string, []byte, []byte) error                      { return nil }
func (f *testFileStore) PutMetadata(string, []byte) error                      { return nil }
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
		FileStore:          &testFileStore{},
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

func TestIndexerBasics_Trie(t *testing.T) {
	testIndexerBasics(t, IndexTypeTrie)
}

func TestIndexerBasics_Flat(t *testing.T) {
	testIndexerBasics(t, IndexTypeFlat)
}

func testIndexerBasics(t *testing.T, indexType IndexType) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, indexType)

	// Test adding files
	metadata1 := types.FileMetadata{
		Name:        "test1.txt",
		Path:        "/docs/test1.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "abc123",
	}
	idx.AddFile("/docs/test1.txt", metadata1)

	metadata2 := types.FileMetadata{
		Name:        "test2.txt",
		Path:        "/docs/test2.txt",
		Size:        200,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "def456",
	}
	idx.AddFile("/docs/test2.txt", metadata2)

	metadata3 := types.FileMetadata{
		Name:        "other.txt",
		Path:        "/other/other.txt",
		Size:        300,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "ghi789",
	}
	idx.AddFile("/other/other.txt", metadata3)

	for _, path := range []string{"/docs/test1.txt", "/docs/test2.txt", "/other/other.txt"} {
		partitionID := types.PartitionIDForPath(path)
		partitionPaths := idx.FilesForPartition(partitionID)
		if !containsPath(partitionPaths, path) {
			t.Errorf("[%s] Expected partition %s to contain %s", indexType, partitionID, path)
		}
	}

	// Test prefix search
	results := idx.PrefixSearch("/docs/")
	if len(results) != 2 {
		t.Errorf("[%s] Expected 2 results for /docs/ prefix, got %d", indexType, len(results))
	}

	results = idx.PrefixSearch("/other/")
	if len(results) != 1 {
		t.Errorf("[%s] Expected 1 result for /other/ prefix, got %d", indexType, len(results))
	}

	results = idx.PrefixSearch("/")
	if len(results) != 2 {
		t.Errorf("[%s] Expected 2 results for / prefix, got %d", indexType, len(results))
	}

	// Test delete
	idx.DeleteFile("/docs/test1.txt")
	results = idx.PrefixSearch("/docs/")
	if len(results) != 1 {
		t.Errorf("[%s] Expected 1 result after delete, got %d", indexType, len(results))
	}

	deletedPartition := types.PartitionIDForPath("/docs/test1.txt")
	if !containsPath(idx.FilesForPartition(deletedPartition), "/docs/test1.txt") {
		t.Errorf("[%s] Expected partition %s to retain tombstone for /docs/test1.txt", indexType, deletedPartition)
	}
}

func TestIndexerUpdate_Trie(t *testing.T) {
	testIndexerUpdate(t, IndexTypeTrie)
}

func TestIndexerUpdate_Flat(t *testing.T) {
	testIndexerUpdate(t, IndexTypeFlat)
}

func testIndexerUpdate(t *testing.T, indexType IndexType) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, indexType)

	// Add a file
	metadata := types.FileMetadata{
		Name:        "test.txt",
		Path:        "/test.txt",
		Size:        100,
		ContentType: "text/plain",
		ModifiedAt:  time.Now(),
		Checksum:    "abc123",
	}
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
	idx.AddFile("/test.txt", updatedMetadata)

	// Should still have only one result
	results := idx.PrefixSearch("/")
	if len(results) != 1 {
		t.Errorf("[%s] Expected 1 result after update, got %d", indexType, len(results))
	}

	// Check that size was updated
	if results[0].Size != 200 {
		t.Errorf("[%s] Expected size 200 after update, got %d", indexType, results[0].Size)
	}
}

func TestIndexerDeletedFiles_Trie(t *testing.T) {
	testIndexerDeletedFiles(t, IndexTypeTrie)
}

func TestIndexerDeletedFiles_Flat(t *testing.T) {
	testIndexerDeletedFiles(t, IndexTypeFlat)
}

func testIndexerDeletedFiles(t *testing.T, indexType IndexType) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := newTestIndexer(logger, indexType)

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
	idx.AddFile("/deleted.txt", deletedMetadata)

	deletedPartition := types.PartitionIDForPath("/deleted.txt")
	if !containsPath(idx.FilesForPartition(deletedPartition), "/deleted.txt") {
		t.Errorf("[%s] Expected partition %s to include /deleted.txt tombstone", indexType, deletedPartition)
	}

	// Search should not return deleted files
	results := idx.PrefixSearch("/")
	if len(results) != 1 {
		t.Errorf("[%s] Expected 1 result (deleted files filtered), got %d", indexType, len(results))
	}

	if results[0].Path != "/test.txt" {
		t.Errorf("[%s] Expected /test.txt, got %s", indexType, results[0].Path)
	}
}

func TestIndexType(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)

	// Test default (should be trie)
	idx := NewIndexer(logger, newTestApp(logger))
	if idx.GetIndexType() != IndexTypeTrie {
		t.Errorf("Expected default index type to be trie, got %s", idx.GetIndexType())
	}

	// Test explicit trie
	idxTrie := newTestIndexer(logger, IndexTypeTrie)
	if idxTrie.GetIndexType() != IndexTypeTrie {
		t.Errorf("Expected trie index type, got %s", idxTrie.GetIndexType())
	}

	// Test explicit flat
	idxFlat := newTestIndexer(logger, IndexTypeFlat)
	if idxFlat.GetIndexType() != IndexTypeFlat {
		t.Errorf("Expected flat index type, got %s", idxFlat.GetIndexType())
	}
}

func BenchmarkPrefixSearch_Trie(b *testing.B) {
	benchmarkPrefixSearch(b, IndexTypeTrie)
}

func BenchmarkPrefixSearch_Flat(b *testing.B) {
	benchmarkPrefixSearch(b, IndexTypeFlat)
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
