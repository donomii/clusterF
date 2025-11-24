// indexer.go - In-memory file index for fast searching with multiple implementations
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/frogpond"
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
	partitionFiles     map[types.PartitionID]map[string]struct{} // partition -> all known paths (including tombstones)
	pathToPartition    map[string]types.PartitionID              // path -> partition
	pathMetadata       map[string]types.FileMetadata             // active path -> metadata
	partitionModLatest map[types.PartitionID]time.Time           // partition -> latest ModifiedAt
	partitionActive    map[types.PartitionID]map[string]struct{} // partition -> active (non-deleted) paths

	// Partition membership coordination
	frogpond        *frogpond.Node
	nodeID          types.NodeID
	sendUpdates     func([]frogpond.DataPoint)
	noStore         bool
	suppressUpdates atomic.Bool
}

// NewIndexer creates a new in-memory file indexer with trie-based index (default)
func NewIndexer(logger *log.Logger) *Indexer {
	return NewIndexerWithType(logger, IndexTypeTrie)
}

// NewIndexerWithType creates a new indexer with a specific implementation
func NewIndexerWithType(logger *log.Logger, indexType IndexType) *Indexer {
	idx := &Indexer{
		indexType:          indexType,
		logger:             logger,
		partitionFiles:     make(map[types.PartitionID]map[string]struct{}),
		pathToPartition:    make(map[string]types.PartitionID),
		pathMetadata:       make(map[string]types.FileMetadata),
		partitionModLatest: make(map[types.PartitionID]time.Time),
		partitionActive:    make(map[types.PartitionID]map[string]struct{}),
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

// ensurePartitionEntryLocked lazily initialises the per-partition path set.
func (idx *Indexer) ensurePartitionEntryLocked(partitionID types.PartitionID) map[string]struct{} {
	if paths, ok := idx.partitionFiles[partitionID]; ok {
		return paths
	}
	paths := make(map[string]struct{})
	idx.partitionFiles[partitionID] = paths
	return paths
}

// trackPartitionPathLocked records that a live (non-deleted) path belongs to the partition.
func (idx *Indexer) trackPartitionPathLocked(partitionID types.PartitionID, path string) {
	paths := idx.ensurePartitionEntryLocked(partitionID)
	paths[path] = struct{}{}
	idx.pathToPartition[path] = partitionID
}

func (idx *Indexer) ensureActivePartitionEntryLocked(partitionID types.PartitionID) map[string]struct{} {
	if paths, ok := idx.partitionActive[partitionID]; ok {
		return paths
	}
	paths := make(map[string]struct{})
	idx.partitionActive[partitionID] = paths
	return paths
}

func (idx *Indexer) recomputePartitionLatestLocked(partitionID types.PartitionID) {
	active := idx.partitionActive[partitionID]
	latest := time.Time{}
	for path := range active {
		if meta, ok := idx.pathMetadata[path]; ok && meta.ModifiedAt.After(latest) {
			latest = meta.ModifiedAt
		}
	}
	if latest.IsZero() {
		delete(idx.partitionModLatest, partitionID)
	} else {
		idx.partitionModLatest[partitionID] = latest
	}
}

func (idx *Indexer) addActivePathLocked(partitionID types.PartitionID, path string, metadata types.FileMetadata) {
	active := idx.ensureActivePartitionEntryLocked(partitionID)
	active[path] = struct{}{}

	metaCopy := metadata
	if metaCopy.ModifiedAt.IsZero() {
		metaCopy.ModifiedAt = time.Now()
	}
	idx.pathMetadata[path] = metaCopy
	idx.recomputePartitionLatestLocked(partitionID)
}

func (idx *Indexer) removeActivePathLocked(partitionID types.PartitionID, path string) {
	if active, ok := idx.partitionActive[partitionID]; ok {
		delete(active, path)
		if len(active) == 0 {
			delete(idx.partitionActive, partitionID)
			delete(idx.partitionModLatest, partitionID)
		} else {
			idx.recomputePartitionLatestLocked(partitionID)
		}
	}
	delete(idx.pathMetadata, path)
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
		idx.removeActivePathLocked(partitionID, effectivePath)
		idx.removeFromSearchLocked(effectivePath)
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
			idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
		}
		return
	}

	idx.addActivePathLocked(partitionID, effectivePath, metadata)
	idx.upsertSearchLocked(effectivePath, metadata)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
	}
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	partitionID := idx.partitionForPath(path)
	idx.trackPartitionPathLocked(partitionID, path)
	idx.removeActivePathLocked(partitionID, path)
	idx.removeFromSearchLocked(path)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
	}
}

// FilesForPartition returns the tracked paths for a partition in sorted order.
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

// updatePartitionMembershipLocked pushes our holder state for the partition into the CRDT.
func (idx *Indexer) updatePartitionMembershipLocked(partitionID types.PartitionID) error {
	if idx.suppressUpdates.Load() || idx.noStore {
		return nil
	}

	if idx.frogpond == nil || idx.sendUpdates == nil || idx.nodeID == "" {
		return nil
	}

	holderKey := fmt.Sprintf("partitions/%s/holders/%s", partitionID, idx.nodeID)
	paths := idx.partitionActive[partitionID]

	if len(paths) == 0 {
		updates := idx.frogpond.DeleteDataPoint(holderKey, 30*time.Minute)
		if len(updates) > 0 {
			idx.sendUpdates(updates)
		}
		return nil
	}

	modTime := idx.partitionModLatest[partitionID]
	if modTime.IsZero() {
		for path := range paths {
			if meta, ok := idx.pathMetadata[path]; ok && meta.ModifiedAt.After(modTime) {
				modTime = meta.ModifiedAt
			}
		}
		if modTime.IsZero() {
			modTime = time.Now()
		}
		idx.partitionModLatest[partitionID] = modTime
	}

	holder := types.HolderData{
		MostRecentModifiedTime: modTime,
		File_count:             len(paths),
		Checksum:               "",
	}

	payload, err := json.Marshal(holder)
	if err != nil {
		return fmt.Errorf("marshal holder data for %s: %w", partitionID, err)
	}

	updates := idx.frogpond.SetDataPoint(holderKey, payload)
	if len(updates) > 0 {
		idx.sendUpdates(updates)
	}
	return nil
}

// publishAllPartitionMembershipLocked reapplies holder claims for every tracked partition.
func (idx *Indexer) publishAllPartitionMembershipLocked() error {
	if idx.noStore {
		return nil
	}
	var errs []error
	for partitionID := range idx.partitionFiles {
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ConfigurePartitionMembership wires the indexer to update partition claims in the CRDT.
func (idx *Indexer) ConfigurePartitionMembership(frogpondNode *frogpond.Node, nodeID types.NodeID, sendUpdates func([]frogpond.DataPoint), noStore bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.frogpond = frogpondNode
	idx.nodeID = nodeID
	idx.sendUpdates = sendUpdates
	idx.noStore = noStore
}

// ImportFilestore imports all files from a PartitionManagerLike into the index
func (idx *Indexer) ImportFilestore(ctx context.Context, pm types.PartitionManagerLike) error {
	idx.logger.Printf("[INDEXER] Starting import of filestore (type: %s)", idx.indexType)

	idx.suppressUpdates.Store(true)

	total := 0
	active := 0
	err := pm.ScanAllFiles(func(filePath string, metadata types.FileMetadata) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		total++
		idx.AddFile(filePath, metadata)

		if !metadata.Deleted {
			active++
		}
		return nil
	})

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.suppressUpdates.Store(false)
	var publishErr error
	if err == nil {
		publishErr = idx.publishAllPartitionMembershipLocked()
	}

	if err != nil {
		idx.logger.Printf("[INDEXER] Import failed after %d files: %v", total, err)
		return err
	}
	if publishErr != nil {
		return fmt.Errorf("publish partition membership: %w", publishErr)
	}

	idx.logger.Printf("[INDEXER] Import complete: processed %d files (%d active)", total, active)
	return nil
}

// GetIndexType returns the current index type
func (idx *Indexer) GetIndexType() IndexType {
	return idx.indexType
}
