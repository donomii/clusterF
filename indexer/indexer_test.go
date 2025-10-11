// indexer_test.go - Tests for the file indexer
package indexer

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func TestIndexerBasics(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := NewIndexer(logger)

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
		t.Errorf("Expected 2 results for /docs/ prefix, got %d", len(results))
	}

	results = idx.PrefixSearch("/other/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result for /other/ prefix, got %d", len(results))
	}

	results = idx.PrefixSearch("/")
	if len(results) != 3 {
		t.Errorf("Expected 3 results for / prefix, got %d", len(results))
	}

	// Test delete
	idx.DeleteFile("/docs/test1.txt")
	results = idx.PrefixSearch("/docs/")
	if len(results) != 1 {
		t.Errorf("Expected 1 result after delete, got %d", len(results))
	}
}

func TestIndexerUpdate(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := NewIndexer(logger)

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
		t.Errorf("Expected 1 result after update, got %d", len(results))
	}

	// Check that size was updated
	if results[0].Size != 200 {
		t.Errorf("Expected size 200 after update, got %d", results[0].Size)
	}
}

func TestIndexerDeletedFiles(t *testing.T) {
	logger := log.New(os.Stdout, "[TEST] ", log.LstdFlags)
	idx := NewIndexer(logger)

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
		t.Errorf("Expected 1 result (deleted files filtered), got %d", len(results))
	}

	if results[0].Path != "/test.txt" {
		t.Errorf("Expected /test.txt, got %s", results[0].Path)
	}
}
