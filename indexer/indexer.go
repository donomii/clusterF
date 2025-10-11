// indexer.go - In-memory file index for fast searching
package indexer

import (
	"log"
	"strings"
	"sync"

	"github.com/donomii/clusterF/types"
)

// Indexer maintains an in-memory index of files for fast searching
type Indexer struct {
	mu     sync.RWMutex
	files  map[string]types.FileMetadata // path -> metadata
	logger *log.Logger
}

// NewIndexer creates a new in-memory file indexer
func NewIndexer(logger *log.Logger) *Indexer {
	return &Indexer{
		files:  make(map[string]types.FileMetadata),
		logger: logger,
	}
}

// PrefixSearch returns all files matching a path prefix
func (idx *Indexer) PrefixSearch(prefix string) []types.SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []types.SearchResult

	for path, metadata := range idx.files {
		if strings.HasPrefix(path, prefix) {
			if metadata.Deleted {
				continue
			}

			results = append(results, types.SearchResult{
				Name:        metadata.Name,
				Path:        path,
				Size:        metadata.Size,
				ContentType: metadata.ContentType,
				ModifiedAt:  metadata.ModifiedAt,
				CreatedAt:   metadata.CreatedAt,
				Checksum:    metadata.Checksum,
			})
		}
	}

	return results
}

// AddFile adds or updates a file in the index
func (idx *Indexer) AddFile(path string, metadata types.FileMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.files[path] = metadata
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	delete(idx.files, path)
}

// ImportFilestore imports all files from a PartitionManagerLike into the index
func (idx *Indexer) ImportFilestore(pm types.PartitionManagerLike) error {
	idx.logger.Printf("[INDEXER] Starting import of filestore")

	count := 0
	err := pm.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		if !metadata.Deleted {
			idx.AddFile(filePath, metadata)
			count++
		}
		return nil
	})

	if err != nil {
		idx.logger.Printf("[INDEXER] Import failed: %v", err)
		return err
	}

	idx.logger.Printf("[INDEXER] Import complete: indexed %d files", count)
	return nil
}
