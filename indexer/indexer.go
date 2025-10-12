// indexer.go - In-memory file index for fast searching with multiple implementations
package indexer

import (
	"log"
	"strings"
	"sync"

	"github.com/donomii/clusterF/types"
	"github.com/tchap/go-patricia/patricia"
)

// IndexType defines which index implementation to use
type IndexType string

const (
	IndexTypeTrie IndexType = "trie" // Trie-based index (default, faster for prefix searches)
	IndexTypeFlat IndexType = "flat" // Flat map-based index (fallback)
)

// Indexer maintains an in-memory index of files for fast searching
type Indexer struct {
	mu        sync.RWMutex
	indexType IndexType
	logger    *log.Logger

	// Trie-based index (default)
	trie *patricia.Trie // Maps file path -> FileMetadata

	// Flat map-based index (fallback)
	files map[string]types.FileMetadata // path -> metadata
}

// NewIndexer creates a new in-memory file indexer with trie-based index (default)
func NewIndexer(logger *log.Logger) *Indexer {
	return NewIndexerWithType(logger, IndexTypeTrie)
}

// NewIndexerWithType creates a new indexer with a specific implementation
func NewIndexerWithType(logger *log.Logger, indexType IndexType) *Indexer {
	idx := &Indexer{
		indexType: indexType,
		logger:    logger,
	}

	switch indexType {
	case IndexTypeTrie:
		idx.trie = patricia.NewTrie()
		idx.logger.Printf("[INDEXER] Using trie-based index")
	case IndexTypeFlat:
		idx.files = make(map[string]types.FileMetadata)
		idx.logger.Printf("[INDEXER] Using flat map-based index")
	default:
		// Default to trie
		idx.trie = patricia.NewTrie()
		idx.indexType = IndexTypeTrie
		idx.logger.Printf("[INDEXER] Unknown index type, defaulting to trie-based index")
	}

	return idx
}

// PrefixSearch returns all files matching a path prefix
func (idx *Indexer) PrefixSearch(prefix string) []types.SearchResult {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	switch idx.indexType {
	case IndexTypeTrie:
		return idx.prefixSearchTrie(prefix)
	case IndexTypeFlat:
		return idx.prefixSearchFlat(prefix)
	default:
		return idx.prefixSearchTrie(prefix)
	}
}

// prefixSearchTrie implements prefix search using the trie
func (idx *Indexer) prefixSearchTrie(prefix string) []types.SearchResult {
	resultMap := make(map[string]types.SearchResult)

	// Visit all entries with the given prefix
	idx.trie.VisitSubtree(patricia.Prefix(prefix), func(path patricia.Prefix, item patricia.Item) error {
		metadata, ok := item.(types.FileMetadata)
		if !ok {
			return nil
		}

		// Skip deleted files
		if metadata.Deleted {
			return nil
		}

		res := types.SearchResult{
			Name:        metadata.Name,
			Path:        string(path),
			Size:        metadata.Size,
			ContentType: metadata.ContentType,
			ModifiedAt:  metadata.ModifiedAt,
			CreatedAt:   metadata.CreatedAt,
			Checksum:    metadata.Checksum,
		}
		types.AddResultToMap(res, resultMap, prefix)

		return nil
	})

	var results []types.SearchResult
	for _, res := range resultMap {
		results = append(results, res)
	}

	return results
}

// prefixSearchFlat implements prefix search using the flat map
func (idx *Indexer) prefixSearchFlat(prefix string) []types.SearchResult {
	resultMap := make(map[string]types.SearchResult)

	for path, metadata := range idx.files {
		if strings.HasPrefix(path, prefix) {
			if metadata.Deleted {
				continue
			}

			res := types.SearchResult{
				Name:        metadata.Name,
				Path:        path,
				Size:        metadata.Size,
				ContentType: metadata.ContentType,
				ModifiedAt:  metadata.ModifiedAt,
				CreatedAt:   metadata.CreatedAt,
				Checksum:    metadata.Checksum,
			}
			types.AddResultToMap(res, resultMap, prefix)
		}
	}

	var results []types.SearchResult
	for _, res := range resultMap {
		results = append(results, res)
	}

	return results
}

// AddFile adds or updates a file in the index
func (idx *Indexer) AddFile(path string, metadata types.FileMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	switch idx.indexType {
	case IndexTypeTrie:
		idx.trie.Insert(patricia.Prefix(path), metadata)
		idx.trie.Set(patricia.Prefix(path), metadata)
	case IndexTypeFlat:
		idx.files[path] = metadata
	default:
		idx.trie.Insert(patricia.Prefix(path), metadata)
		idx.trie.Set(patricia.Prefix(path), metadata)
	}
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	switch idx.indexType {
	case IndexTypeTrie:
		idx.trie.Delete(patricia.Prefix(path))
	case IndexTypeFlat:
		delete(idx.files, path)
	default:
		idx.trie.Delete(patricia.Prefix(path))
	}
}

// ImportFilestore imports all files from a PartitionManagerLike into the index
func (idx *Indexer) ImportFilestore(pm types.PartitionManagerLike) error {
	idx.logger.Printf("[INDEXER] Starting import of filestore (type: %s)", idx.indexType)

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

// GetIndexType returns the current index type
func (idx *Indexer) GetIndexType() IndexType {
	return idx.indexType
}
