// indexer_test.go - Tests for the file indexer
package indexer

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func TestIndexerBasics_Trie(t *testing.T) {
	testIndexerBasics(t, IndexTypeTrie)
}

func TestIndexerBasics_Flat(t *testing.T) {
	testIndexerBasics(t, IndexTypeFlat)
}

func testIndexerBasics(t *testing.T, indexType IndexType) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := NewIndexerWithType(logger, indexType)

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
}

func TestIndexerUpdate_Trie(t *testing.T) {
	testIndexerUpdate(t, IndexTypeTrie)
}

func TestIndexerUpdate_Flat(t *testing.T) {
	testIndexerUpdate(t, IndexTypeFlat)
}

func testIndexerUpdate(t *testing.T, indexType IndexType) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := NewIndexerWithType(logger, indexType)

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
	idx := NewIndexerWithType(logger, indexType)

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
	idx := NewIndexer(logger)
	if idx.GetIndexType() != IndexTypeTrie {
		t.Errorf("Expected default index type to be trie, got %s", idx.GetIndexType())
	}

	// Test explicit trie
	idxTrie := NewIndexerWithType(logger, IndexTypeTrie)
	if idxTrie.GetIndexType() != IndexTypeTrie {
		t.Errorf("Expected trie index type, got %s", idxTrie.GetIndexType())
	}

	// Test explicit flat
	idxFlat := NewIndexerWithType(logger, IndexTypeFlat)
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
	idx := NewIndexerWithType(logger, indexType)

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
