// indexer.go - In-memory file index for fast searching with unique document IDs
package indexer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
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
)

type indexedSearchEntry struct {
	path string
	id   uint64
}

// Indexer maintains an in-memory index of files for fast searching
// Each document is assigned a unique ID (atomic counter) at runtime.
type Indexer struct {
	mu              sync.RWMutex
	indexType       IndexType
	logger          *log.Logger
	backend         searchBackend
	partitionActive map[int][]int // partition number -> docIDs (as int)

	docIDCounter atomic.Uint64

	deps *types.App
}

type searchBackend interface {
	Add(partitionID types.PartitionID, path string, docID uint64)
	Delete(partitionID types.PartitionID, path string)
	PrefixSearch(prefix string) []indexedSearchEntry
	PrefixSearchInferDirectories(prefix string) []indexedSearchEntry
	FilesForPartition(partitionID types.PartitionID) []string
	TrackedPartitions() []types.PartitionID
	PathToDocID(path string) (uint64, bool)
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

func (b *trieBackend) Add(partitionID types.PartitionID, path string, docID uint64) {
	b.trie.Set(patricia.Prefix(path), docID)
	b.trackPartitionPath(partitionID, path)
}

func (b *trieBackend) Delete(partitionID types.PartitionID, path string) {
	b.trie.Delete(patricia.Prefix(path))
	b.trackPartitionPath(partitionID, path) // keep tombstone path in partition listing
}

func (b *trieBackend) PrefixSearch(prefix string) []indexedSearchEntry {
	resultMap := make(map[string]indexedSearchEntry)
	b.trie.VisitSubtree(patricia.Prefix(prefix), func(path patricia.Prefix, item patricia.Item) error {
		docID, ok := item.(uint64)
		if !ok {
			return nil
		}
		key := string(path)
		resultMap[key] = indexedSearchEntry{path: key, id: docID}
		return nil
	})

	results := make([]indexedSearchEntry, 0, len(resultMap))
	for _, res := range resultMap {
		results = append(results, res)
	}

	return results
}

func (b *trieBackend) PrefixSearchInferDirectories(prefix string) []indexedSearchEntry {
	resultMap := make(map[string]indexedSearchEntry)
	b.trie.VisitSubtree(patricia.Prefix(prefix), func(path patricia.Prefix, item patricia.Item) error {
		docID, ok := item.(uint64)
		if !ok {
			return nil
		}
		key := types.CollapseToDirectory(string(path), prefix) //We don't trim the prefix here, we return the full path
		resultMap[key] = indexedSearchEntry{path: key, id: docID}
		return nil
	})

	results := make([]indexedSearchEntry, 0, len(resultMap))
	for _, res := range resultMap {
		results = append(results, res)
	}

	return results //Directories end in a slash, files never do
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

func (b *trieBackend) PathToDocID(path string) (uint64, bool) {
	item := b.trie.Get(patricia.Prefix(path))
	if item == nil {
		return 0, false
	}
	docID, ok := item.(uint64)
	return docID, ok
}

func buildSearchResult(path string, meta types.FileMetadata) types.SearchResult {
	return types.SearchResult{
		Name:        meta.Name,
		Path:        path,
		Size:        meta.Size,
		ContentType: meta.ContentType,
		ModifiedAt:  meta.ModifiedAt,
		CreatedAt:   meta.CreatedAt,
		Checksum:    meta.Checksum,
		Holders:     meta.Holders,
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
		partitionActive: make(map[int][]int),
		deps:            deps,
	}

	switch indexType {
	case IndexTypeTrie:
		idx.backend = newTrieBackend()
		idx.logf("[INDEXER] Using trie-based index")
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

func (idx *Indexer) lock() {
	idx.mu.Lock()
}

func (idx *Indexer) unlock() {
	idx.mu.Unlock()
}

func (idx *Indexer) rlock() {
	idx.mu.RLock()
}

func (idx *Indexer) runlock() {
	idx.mu.RUnlock()
}

func (idx *Indexer) partitionForPath(path string) types.PartitionID {
	return types.PartitionIDForPath(path)
}

func (idx *Indexer) partitionNumber(partitionID types.PartitionID) int {
	trimmed := strings.TrimPrefix(string(partitionID), "p")
	num, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0
	}
	return num
}

func (idx *Indexer) nextDocID() uint64 {
	return idx.docIDCounter.Add(1)
}

func (idx *Indexer) docIDForPath(path string) uint64 {
	if docID, ok := idx.backend.PathToDocID(path); ok {
		return docID
	}
	docID := idx.nextDocID()
	partitionID := types.PartitionIDForPath(path)
	idx.backend.Add(partitionID, path, docID)
	return docID
}

func (idx *Indexer) addDocToPartitionLocked(partitionNumber int, docID uint64) {
	list := idx.partitionActive[partitionNumber]
	idAsInt := int(docID)
	for _, existing := range list {
		if existing == idAsInt {
			idx.partitionActive[partitionNumber] = list
			return
		}
	}
	idx.partitionActive[partitionNumber] = append(list, idAsInt)
}

func (idx *Indexer) removeDocFromPartitionLocked(partitionNumber int, docID uint64) {
	list := idx.partitionActive[partitionNumber]
	if len(list) == 0 {
		return
	}
	idAsInt := int(docID)
	dst := list[:0]
	for _, existing := range list {
		if existing != idAsInt {
			dst = append(dst, existing)
		}
	}
	if len(dst) == 0 {
		delete(idx.partitionActive, partitionNumber)
		return
	}
	idx.partitionActive[partitionNumber] = dst
}
func (idx *Indexer) loadMetadata(path string) (types.FileMetadata, bool) {
	var meta types.FileMetadata
	data, err := idx.deps.FileStore.GetMetadata(path)
	if err != nil || len(data) == 0 {
		return meta, false
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return meta, false
	}
	return meta, true
}

func (idx *Indexer) upsertDocLocked(partitionID types.PartitionID, path string) {
	docID := idx.docIDForPath(path)

	if idx.backend != nil {
		idx.backend.Add(partitionID, path, docID)
	}
	idx.addDocToPartitionLocked(idx.partitionNumber(partitionID), docID)
}

func (idx *Indexer) deleteDocLocked(partitionID types.PartitionID, path string) {
	if docID, ok := idx.backend.PathToDocID(path); ok {
		idx.removeDocFromPartitionLocked(idx.partitionNumber(partitionID), docID)
	}
	if idx.backend != nil {
		idx.backend.Delete(partitionID, path)
	}
}

// PrefixSearch returns all files matching a path prefix
func (idx *Indexer) PrefixSearch(prefix string) []types.SearchResult {
	idx.rlock()
	defer idx.runlock()
	raw := idx.backend.PrefixSearchInferDirectories(prefix)
	if len(raw) == 0 {
		return nil
	}

	resultMap := make(map[string]types.SearchResult)
	for _, entry := range raw {
		if strings.HasSuffix(entry.path, "/") {
			now := time.Now()
			sum := sha256.Sum256([]byte(fmt.Sprintf("%v%v", entry.path, now.UnixNano())))
			meta := types.FileMetadata{
				Name:        filepath.Base(entry.path),
				Path:        entry.path,
				Size:        0,
				ContentType: "application/directory",
				IsDirectory: true,
				Checksum:    hex.EncodeToString(sum[:]),
				CreatedAt:   now,
				ModifiedAt:  now,
			}
			types.AddResultToMap(buildSearchResult(entry.path, meta), resultMap, entry.path, prefix)

		} else {
			meta, ok := idx.loadMetadata(entry.path)
			if !ok || meta.Deleted {
				continue
			}
			types.AddResultToMap(buildSearchResult(entry.path, meta), resultMap, entry.path, prefix)
		}
	}

	results := make([]types.SearchResult, 0, len(resultMap))
	for _, res := range resultMap {
		results = append(results, res)
	}
	return results
}

// AddFile adds or updates a file in the index
func (idx *Indexer) AddFile(path string, metadata types.FileMetadata) {
	idx.lock()
	defer idx.unlock()

	if idx.deps.Cluster.AppContext().Err() != nil {
		return
	}

	//idx.logger.Printf("[INDEXER] Adding file %s", path)

	effectivePath := path
	partitionID := idx.partitionForPath(effectivePath)

	if metadata.Deleted {
		idx.deleteDocLocked(partitionID, effectivePath)
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil && idx.logger != nil {
			idx.logger.Printf("[INDEXER] Failed to update membership for deleted file in %s: %v", partitionID, err)
		}
		return
	}

	idx.upsertDocLocked(partitionID, effectivePath)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil && idx.logger != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for upsert file %s: %v", partitionID, err)
	}
}

// DeleteFile removes a file from the index
func (idx *Indexer) DeleteFile(path string) {
	idx.lock()
	defer idx.unlock()

	partitionID := idx.partitionForPath(path)
	idx.deleteDocLocked(partitionID, path)
	if err := idx.updatePartitionMembershipLocked(partitionID); err != nil && idx.logger != nil {
		idx.logger.Printf("[INDEXER] Failed to update membership for %s: %v", partitionID, err)
	}
}

// FilesForPartition returns the tracked paths for a partition in sorted order.
func (idx *Indexer) FilesForPartition(partitionID types.PartitionID) []string {
	idx.rlock()
	defer idx.runlock()

	if idx.backend == nil {
		return []string{}
	}
	return idx.backend.FilesForPartition(partitionID)
}

// updatePartitionMembershipLocked pushes our holder state for the partition into the CRDT.
func (idx *Indexer) updatePartitionMembershipLocked(partitionID types.PartitionID) error {
	holderKey := fmt.Sprintf("partitions/%s/holders/%s", partitionID, idx.deps.Cluster.ID())
	partitionNumber := idx.partitionNumber(partitionID)
	pathCount := len(idx.partitionActive[partitionNumber])

	if pathCount == 0 {
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
		File_count: pathCount,
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
		panic("index backend missing")
	}
	for _, partitionID := range idx.backend.TrackedPartitions() {
		idx.logger.Printf("[INDEXER] Updating partition membership for %s", partitionID)
		if err := idx.updatePartitionMembershipLocked(partitionID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ImportFilestore imports all files from a PartitionManagerLike into the index
func (idx *Indexer) ImportFilestore(ctx context.Context, pm types.PartitionManagerLike) error {
	idx.logger.Printf("[INDEXER] Starting import of filestore (type: %s)", idx.indexType)

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

	var publishErr error
	if err == nil {
		idx.lock()
		defer idx.unlock()
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
