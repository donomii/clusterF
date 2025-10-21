// indexer.go - In-memory file index for fast searching with multiple implementations
package indexer

import (
	"context"
	"log"
	"sort"
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

	// Partition awareness
	partitionFiles  map[types.PartitionID]map[string]struct{} // partition -> set of paths
	pathToPartition map[string]types.PartitionID              // path -> partition
}

// NewIndexer creates a new in-memory file indexer with trie-based index (default)
func NewIndexer(logger *log.Logger) *Indexer {
	return NewIndexerWithType(logger, IndexTypeTrie)
}

// NewIndexerWithType creates a new indexer with a specific implementation
func NewIndexerWithType(logger *log.Logger, indexType IndexType) *Indexer {
	idx := &Indexer{
		indexType:       indexType,
		logger:          logger,
		partitionFiles:  make(map[types.PartitionID]map[string]struct{}),
		pathToPartition: make(map[string]types.PartitionID),
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

func (idx *Indexer) partitionForPath(path string) types.PartitionID {
	return types.PartitionIDForPath(path)
}

func (idx *Indexer) ensurePartitionEntryLocked(partitionID types.PartitionID) map[string]struct{} {
	if paths, ok := idx.partitionFiles[partitionID]; ok {
		return paths
	}
	paths := make(map[string]struct{})
	idx.partitionFiles[partitionID] = paths
	return paths
}

func (idx *Indexer) trackPartitionPathLocked(partitionID types.PartitionID, path string) {
	paths := idx.ensurePartitionEntryLocked(partitionID)
	paths[path] = struct{}{}
	idx.pathToPartition[path] = partitionID
}

func (idx *Indexer) removeFromSearchLocked(path string) {
	switch idx.indexType {
	case IndexTypeTrie:
		if idx.trie != nil {
			idx.trie.Delete(patricia.Prefix(path))
		}
	case IndexTypeFlat:
		delete(idx.files, path)
	default:
		if idx.trie != nil {
			idx.trie.Delete(patricia.Prefix(path))
		}
	}
}

func (idx *Indexer) upsertSearchLocked(path string, metadata types.FileMetadata) {
	switch idx.indexType {
	case IndexTypeTrie:
		if idx.trie != nil {
			idx.trie.Insert(patricia.Prefix(path), metadata)
			idx.trie.Set(patricia.Prefix(path), metadata)
		}
	case IndexTypeFlat:
		if idx.files == nil {
			idx.files = make(map[string]types.FileMetadata)
		}
		idx.files[path] = metadata
	default:
		if idx.trie != nil {
			idx.trie.Insert(patricia.Prefix(path), metadata)
			idx.trie.Set(patricia.Prefix(path), metadata)
		}
	}
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

	effectivePath := path
	if metadata.Path != "" {
		effectivePath = metadata.Path
	} else {
		metadata.Path = effectivePath
	}

	partitionID := idx.partitionForPath(effectivePath)
	idx.trackPartitionPathLocked(partitionID, effectivePath)

	if metadata.Deleted {
		idx.removeFromSearchLocked(effectivePath)
		return
	}

	idx.upsertSearchLocked(effectivePath, metadata)
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	partitionID := idx.partitionForPath(path)
	idx.trackPartitionPathLocked(partitionID, path)
	idx.removeFromSearchLocked(path)
}

// FilesForPartition returns the tracked paths for a partition in sorted order
func (idx *Indexer) FilesForPartition(partitionID types.PartitionID) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	pathsMap, ok := idx.partitionFiles[partitionID]
	if !ok || len(pathsMap) == 0 {
		return []string{}
	}

	paths := make([]string, 0, len(pathsMap))
	for path := range pathsMap {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// ImportFilestore imports all files from a PartitionManagerLike into the index
func (idx *Indexer) ImportFilestore(ctx context.Context, pm types.PartitionManagerLike) error {
	idx.logger.Printf("[INDEXER] Starting import of filestore (type: %s)", idx.indexType)

	total := 0
	active := 0
	err := pm.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		total++
		idx.AddFile(filePath, metadata)

		if !metadata.Deleted {
			active++
		}
		return nil
	})

	if err != nil {
		idx.logger.Printf("[INDEXER] Import failed: %v", err)
		return err
	}

	idx.logger.Printf("[INDEXER] Import complete: processed %d files (%d active)", total, active)
	return nil
}

// GetIndexType returns the current index type
func (idx *Indexer) GetIndexType() IndexType {
	return idx.indexType
}
