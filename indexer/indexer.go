// indexer.go - In-memory file index for fast searching with multiple implementations
package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	backend   searchBackend

	// Partition awareness
	pathMetadata    map[string]types.FileMetadata             // active path -> metadata
	partitionActive map[types.PartitionID]map[string]struct{} // partition -> active (non-deleted) paths

	// Partition membership coordination

	suppressUpdates atomic.Bool
	deps            *types.App
}

type searchBackend interface {
	Add(partitionID types.PartitionID, path string, metadata types.FileMetadata)
	Delete(partitionID types.PartitionID, path string)
	PrefixSearch(prefix string) []types.SearchResult
	FilesForPartition(partitionID types.PartitionID) []string
	TrackedPartitions() []types.PartitionID
}

type trieBackend struct {
	trie           *patricia.Trie
	partitionTries map[types.PartitionID]*patricia.Trie
}

func newTrieBackend() *trieBackend {
	return &trieBackend{
		trie:           patricia.NewTrie(),
		partitionTries: make(map[types.PartitionID]*patricia.Trie),
	}
}

func (b *trieBackend) ensurePartitionTrie(partitionID types.PartitionID) *patricia.Trie {
	if t, ok := b.partitionTries[partitionID]; ok {
		return t
	}
	trie := patricia.NewTrie()
	b.partitionTries[partitionID] = trie
	return trie
}

func (b *trieBackend) trackPartitionPath(partitionID types.PartitionID, path string) {
	trie := b.ensurePartitionTrie(partitionID)
	trie.Set(patricia.Prefix(path), struct{}{})
}

func (b *trieBackend) Add(partitionID types.PartitionID, path string, metadata types.FileMetadata) {
	if b.trie == nil {
		b.trie = patricia.NewTrie()
	}
	b.trie.Set(patricia.Prefix(path), metadata)
	b.trackPartitionPath(partitionID, path)
}

func (b *trieBackend) Delete(partitionID types.PartitionID, path string) {
	if b.trie == nil {
		return
	}
	b.trie.Delete(patricia.Prefix(path))
	b.trackPartitionPath(partitionID, path)
}

func (b *trieBackend) PrefixSearch(prefix string) []types.SearchResult {
	if b.trie == nil {
		return nil
	}

	resultMap := make(map[string]types.SearchResult)
	b.trie.VisitSubtree(patricia.Prefix(prefix), func(path patricia.Prefix, item patricia.Item) error {
		metadata, ok := item.(types.FileMetadata)
		if !ok || metadata.Deleted {
			return nil
		}
		types.AddResultToMap(buildSearchResult(string(path), metadata), resultMap, prefix)
		return nil
	})

	results := make([]types.SearchResult, 0, len(resultMap))
	for _, res := range resultMap {
		results = append(results, res)
	}
	return results
}

type flatBackend struct {
	files          map[string]types.FileMetadata
	partitionPaths map[types.PartitionID]map[string]struct{}
}

func newFlatBackend() *flatBackend {
	return &flatBackend{
		files:          make(map[string]types.FileMetadata),
		partitionPaths: make(map[types.PartitionID]map[string]struct{}),
	}
}

func (b *flatBackend) trackPartitionPath(partitionID types.PartitionID, path string) {
	paths, ok := b.partitionPaths[partitionID]
	if !ok {
		paths = make(map[string]struct{})
		b.partitionPaths[partitionID] = paths
	}
	paths[path] = struct{}{}
}

func (b *flatBackend) Add(partitionID types.PartitionID, path string, metadata types.FileMetadata) {
	if b.files == nil {
		b.files = make(map[string]types.FileMetadata)
	}
	b.files[path] = metadata
	b.trackPartitionPath(partitionID, path)
}

func (b *flatBackend) Delete(partitionID types.PartitionID, path string) {
	if b.files == nil {
		return
	}
	delete(b.files, path)
	b.trackPartitionPath(partitionID, path)
}

func (b *flatBackend) PrefixSearch(prefix string) []types.SearchResult {
	if len(b.files) == 0 {
		return nil
	}
	resultMap := make(map[string]types.SearchResult)
	for path, metadata := range b.files {
		if !strings.HasPrefix(path, prefix) || metadata.Deleted {
			continue
		}
		types.AddResultToMap(buildSearchResult(path, metadata), resultMap, prefix)
	}
	results := make([]types.SearchResult, 0, len(resultMap))
	for _, res := range resultMap {
		results = append(results, res)
	}
	return results
}

func (b *trieBackend) FilesForPartition(partitionID types.PartitionID) []string {
	trie := b.partitionTries[partitionID]
	if trie == nil {
		return []string{}
	}
	var paths []string
	trie.Visit(func(prefix patricia.Prefix, _ patricia.Item) error {
		paths = append(paths, string(prefix))
		return nil
	})
	sort.Strings(paths)
	return paths
}

func (b *trieBackend) TrackedPartitions() []types.PartitionID {
	partitions := make([]types.PartitionID, 0, len(b.partitionTries))
	for pid := range b.partitionTries {
		partitions = append(partitions, pid)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

func (b *flatBackend) FilesForPartition(partitionID types.PartitionID) []string {
	pathsMap := b.partitionPaths[partitionID]
	if len(pathsMap) == 0 {
		return []string{}
	}
	paths := make([]string, 0, len(pathsMap))
	for path := range pathsMap {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func (b *flatBackend) TrackedPartitions() []types.PartitionID {
	partitions := make([]types.PartitionID, 0, len(b.partitionPaths))
	for pid := range b.partitionPaths {
		partitions = append(partitions, pid)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions
}

func buildSearchResult(path string, metadata types.FileMetadata) types.SearchResult {
	return types.SearchResult{
		Name:        metadata.Name,
		Path:        path,
		Size:        metadata.Size,
		ContentType: metadata.ContentType,
		ModifiedAt:  metadata.ModifiedAt,
		CreatedAt:   metadata.CreatedAt,
		Checksum:    metadata.Checksum,
	}
}

// NewIndexer creates a new in-memory file indexer with trie-based index (default)
func NewIndexer(logger *log.Logger, deps *types.App) *Indexer {
	return NewIndexerWithType(logger, IndexTypeTrie, deps)
}

// NewIndexerWithType creates a new indexer with a specific implementation
func NewIndexerWithType(logger *log.Logger, indexType IndexType, deps *types.App) *Indexer {
	idx := &Indexer{
		indexType:       indexType,
		logger:          logger,
		pathMetadata:    make(map[string]types.FileMetadata),
		partitionActive: make(map[types.PartitionID]map[string]struct{}),
		deps:            deps,
	}

	switch indexType {
	case IndexTypeTrie:
		idx.backend = newTrieBackend()
		idx.logf("[INDEXER] Using trie-based index")
	case IndexTypeFlat:
		idx.backend = newFlatBackend()
		idx.logf("[INDEXER] Using flat map-based index")
	default:
		idx.backend = newTrieBackend()
		idx.indexType = IndexTypeTrie
		idx.logf("[INDEXER] Unknown index type, defaulting to trie-based index")
	}

	return idx
}

func (idx *Indexer) logf(format string, args ...interface{}) {
	if idx.logger != nil {
		idx.logger.Printf(format, args...)
	}
}

func (idx *Indexer) callerName() string {
	if pc, _, _, ok := runtime.Caller(2); ok {
		if fn := runtime.FuncForPC(pc); fn != nil {
			return fn.Name()
		}
	}
	return "unknown"
}

func (idx *Indexer) lock() {
	idx.logf("[INDEXER] idx.mu locked (write) by %s", idx.callerName())
	idx.mu.Lock()
}

func (idx *Indexer) unlock() {
	idx.mu.Unlock()
	idx.logf("[INDEXER] idx.mu unlocked (write) by %s", idx.callerName())
}

func (idx *Indexer) rlock() {
	idx.logf("[INDEXER] idx.mu locked (read) by %s", idx.callerName())
	idx.mu.RLock()
}

func (idx *Indexer) runlock() {
	idx.mu.RUnlock()
	idx.logf("[INDEXER] idx.mu unlocked (read) by %s", idx.callerName())
}

func (idx *Indexer) partitionForPath(path string) types.PartitionID {
	return types.PartitionIDForPath(path)
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
		} else {
			idx.recomputePartitionLatestLocked(partitionID)
		}
	}
	delete(idx.pathMetadata, path)
}

func (idx *Indexer) removeFromSearchLocked(partitionID types.PartitionID, path string) {
	if idx.backend != nil {
		idx.backend.Delete(partitionID, path)
	}
}

func (idx *Indexer) upsertSearchLocked(partitionID types.PartitionID, path string, metadata types.FileMetadata) {
	if idx.backend != nil {
		idx.backend.Add(partitionID, path, metadata)
	}
}

// PrefixSearch returns all files matching a path prefix
func (idx *Indexer) PrefixSearch(prefix string) []types.SearchResult {
	idx.rlock()
	defer idx.runlock()

	if idx.backend == nil {
		return nil
	}
	return idx.backend.PrefixSearch(prefix)
}

// AddFile adds or updates a file in the index
func (idx *Indexer) AddFile(path string, metadata types.FileMetadata) {
	idx.lock()
	defer idx.unlock()

	effectivePath := path
	if metadata.Path != "" {
		effectivePath = metadata.Path
	} else {
		metadata.Path = effectivePath
	}

	partitionID := idx.partitionForPath(effectivePath)

	if metadata.Deleted {
		idx.removeActivePathLocked(partitionID, effectivePath)
		idx.removeFromSearchLocked(partitionID, effectivePath)
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
			idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
		}
		return
	}

	idx.addActivePathLocked(partitionID, effectivePath, metadata)
	idx.upsertSearchLocked(partitionID, effectivePath, metadata)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
	}
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.lock()
	defer idx.unlock()

	partitionID := idx.partitionForPath(path)
	idx.removeActivePathLocked(partitionID, path)
	idx.removeFromSearchLocked(partitionID, path)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
	}
}

// FilesForPartition returns the tracked paths for a partition in sorted order.
func (idx *Indexer) FilesForPartition(partitionID types.PartitionID) []string {
	// Print entire callstack here to help with debugging
	idx.logf("[INDEXER] FilesForPartition(%s) called by %s", partitionID, idx.callerName())
	stack := make([]byte, 4096)
	runtime.Stack(stack, false)
	idx.logf("[INDEXER] FilesForPartition(%s) stack: %s", partitionID, string(stack))

	idx.rlock()
	defer idx.runlock()

	if idx.backend == nil {
		return []string{}
	}
	return idx.backend.FilesForPartition(partitionID)
}

// updatePartitionMembershipLocked pushes our holder state for the partition into the CRDT.
func (idx *Indexer) updatePartitionMembershipLocked(partitionID types.PartitionID) error {
	if idx.suppressUpdates.Load() {
		return nil
	}

	holderKey := fmt.Sprintf("partitions/%s/holders/%s", partitionID, idx.deps.Cluster.ID())
	paths := idx.partitionActive[partitionID]

	if len(paths) == 0 {
		updates := idx.deps.Frogpond.DeleteDataPoint(holderKey, 30*time.Minute)
		if len(updates) > 0 {
			idx.deps.SendUpdatesToPeers(updates)
		}
		return nil
	}

	checksum, err := idx.deps.Cluster.PartitionManager().CalculatePartitionChecksum(idx.deps.Cluster.AppContext(), partitionID)
	if err != nil {
		return err
	}
	holder := types.HolderData{
		File_count: len(paths),
		Checksum:   checksum,
	}

	payload, err := json.Marshal(holder)
	if err != nil {
		return fmt.Errorf("marshal holder data for %s: %w", partitionID, err)
	}

	updates := idx.deps.Frogpond.SetDataPoint(holderKey, payload)
	if len(updates) > 0 {
		idx.deps.SendUpdatesToPeers(updates)
	}
	return nil
}

// publishAllPartitionMembershipLocked reapplies holder claims for every tracked partition.
func (idx *Indexer) publishAllPartitionMembershipLocked() error {

	var errs []error
	if idx.backend == nil {
		panic("wtf")
	}
	for _, partitionID := range idx.backend.TrackedPartitions() {
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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

	idx.lock()
	defer idx.unlock()
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
