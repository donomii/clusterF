// partitions.go Partitioning system for scalable file storage using existing KV stores
package partitionmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/tchap/go-patricia/patricia"
)

const (
	defaultReplicationFactor = 3
)

type PartitionManager struct {
	deps            *types.App
	ReindexList     *syncmap.SyncMap[types.PartitionID, bool]
	SyncList        *syncmap.SyncMap[types.PartitionID, bool]
	fileSyncMu      sync.RWMutex
	fileSyncTrie    *patricia.Trie
	localPartitions *syncmap.SyncMap[types.PartitionID, bool]
}

type PartitionVersion int64

func NewPartitionManager(deps *types.App) *PartitionManager {
	return &PartitionManager{
		deps:            deps,
		ReindexList:     syncmap.NewSyncMap[types.PartitionID, bool](),
		SyncList:        syncmap.NewSyncMap[types.PartitionID, bool](),
		fileSyncTrie:    patricia.NewTrie(),
		localPartitions: syncmap.NewSyncMap[types.PartitionID, bool](),
	}
}

func (pm *PartitionManager) RecordEssentialDiskActivity() {
	types.Assert(pm.deps.Cluster != nil, "Cluster must not be nil")
	pm.deps.Cluster.RecordDiskActivity(types.DiskActivityEssential)
}

func (pm *PartitionManager) MarkForReindex(pId types.PartitionID, reason string) {
	pm.ReindexList.Store(pId, true)
	pm.logf("[MarkForReindex] Marked partition %v for reindex, because %s", pId, reason)
}

// updateLocalPartitionMembership toggles this node's local holder state and publishes to nodes/<node>/partitions.
func (pm *PartitionManager) updateLocalPartitionMembership(partitionID types.PartitionID, holds bool) {
	if holds {
		if pm.addLocalPartition(partitionID) {
			pm.publishLocalPartitions()
		}
		return
	}
	if pm.removeLocalPartition(partitionID) {
		pm.publishLocalPartitions()
	}
}

// MarkForSync flags a partition to be synced on the next PeriodicPartitionCheck cycle.
func (pm *PartitionManager) MarkForSync(pId types.PartitionID, reason string) {
	pm.SyncList.Store(pId, true)
	pm.logf("[MarkForSync] Marked partition %v for sync, because %s", pId, reason)
}

// SyncListPendingCount returns the number of partitions currently flagged for sync.
func (pm *PartitionManager) SyncListPendingCount() int {
	return pm.countFlagged(pm.SyncList)
}

// ReindexListPendingCount returns the number of partitions currently flagged for reindex.
func (pm *PartitionManager) ReindexListPendingCount() int {
	return pm.countFlagged(pm.ReindexList)
}

func (pm *PartitionManager) countFlagged(list *syncmap.SyncMap[types.PartitionID, bool]) int {
	count := 0
	list.Range(func(_ types.PartitionID, flagged bool) bool {
		if flagged {
			count++
		}
		return true
	})
	return count
}

func (pm *PartitionManager) RunReindex(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		return
	}

	start := time.Now()
	pm.debugf("[REINDEX] Found %v partitions to reindex: %v", pm.ReindexList.Len(), pm.ReindexList.Keys())
	count := 0

	keys := pm.ReindexList.Keys()
	for _, key := range keys {
		if ctx.Err() != nil {
			return
		}
		if pm.getPartitionSyncPaused() {
			return
		}
		value, _ := pm.ReindexList.Load(key)
		if value {
			pm.ReindexList.Store(key, false) // Clear the flag before re-indexing, as new items will not necessarily be caught during
			//pm.deps.Logger.Printf("[REINDEX] Starting reindex of partition %v", key)
			pm.updatePartitionMetadata(ctx, key)
			count = count + 1
			pm.debugf("Finished reindex of %v", key)
		}
	}

	pm.debugf("[RUNREINDEX] Completed reindex cycle in %v for %v partitions", time.Since(start), count)

}

// RemoveNodeFromPartitionWithTimestamp removes a node from a partition holder list with backdated timestamp
func (pm *PartitionManager) RemoveNodeFromPartitionWithTimestamp(nodeID types.NodeID, partitionName string, backdatedTime time.Time) error {
	// Membership is driven by nodes/<node>/partitions; only update local state.
	if nodeID == pm.deps.NodeID {
		pm.updateLocalPartitionMembership(types.PartitionID(partitionName), false)
		pm.debugf("[PARTITION] Removed local node %s from partition %s with backdated timestamp %s", nodeID, partitionName, backdatedTime.Format(time.RFC3339))
	} else {
		pm.debugf("[PARTITION] Ignoring request to remove remote node %s from partition %s; membership is published by the owner", nodeID, partitionName)
	}
	return nil
}

func (pm *PartitionManager) debugf(format string, args ...interface{}) string {
	if pm.deps.Debugf != nil {
		// Get caller info
		_, file, line, ok := runtime.Caller(1) // 1 = caller of debugf
		loc := ""
		if ok {
			loc = fmt.Sprintf("%s:%d: ", file, line)
		}
		message := fmt.Sprintf(loc+format, args...)
		pm.deps.Debugf(message)
		return message
	}
	return ""
}

func (pm *PartitionManager) logf(format string, args ...interface{}) string {
	if pm.deps.Logger != nil {
		// Get caller info
		_, file, line, ok := runtime.Caller(1)
		// Get the last component of the path, the file name
		fileName := filepath.Base(file)
		loc := ""
		if ok {
			loc = fmt.Sprintf("%s:%d: ", fileName, line)
		}
		message := fmt.Sprintf(loc+format, args...)
		pm.deps.Logger.Printf("%v", message)
		return message
	}
	return ""
}

func (pm *PartitionManager) addLocalPartition(partitionID types.PartitionID) bool {
	types.Assertf(partitionID != "", "partitionID must not be empty when adding a local partition")
	if _, exists := pm.localPartitions.Load(partitionID); exists {
		return false
	}
	pm.localPartitions.Store(partitionID, true)
	return true
}

func (pm *PartitionManager) removeLocalPartition(partitionID types.PartitionID) bool {
	types.Assertf(partitionID != "", "partitionID must not be empty when removing a local partition")
	if _, exists := pm.localPartitions.Load(partitionID); exists {
		pm.localPartitions.Delete(partitionID)
		return true
	}
	return false
}

func partitionNumberFromID(partitionID types.PartitionID) (int, bool) {
	if !strings.HasPrefix(string(partitionID), "p") {
		return 0, false
	}
	numStr := strings.TrimPrefix(string(partitionID), "p")
	num, err := strconv.Atoi(numStr)
	if err != nil || num < 0 || num >= types.DefaultPartitionCount {
		return 0, false
	}
	return num, true
}

func (pm *PartitionManager) publishLocalPartitions() {
	if pm.deps.Cluster.NoStore() {
		return
	}

	partitions := []int{}
	pm.localPartitions.Range(func(pid types.PartitionID, present bool) bool {
		if !present {
			return true
		}
		if num, ok := partitionNumberFromID(pid); ok {
			partitions = append(partitions, num)
		}
		return true
	})

	sort.Ints(partitions)
	payload, _ := json.Marshal(partitions)
	key := fmt.Sprintf("nodes/%s/partitions", pm.deps.NodeID)
	updates := pm.deps.Frogpond.SetDataPoint(key, payload)
	pm.deps.SendUpdatesToPeers(updates)
}

func (pm *PartitionManager) recordPartitionTimestamp(partitionID types.PartitionID, filename string, ts time.Time) {
	fs, ok := pm.deps.FileStore.(*DiskFileStore)
	types.Assertf(ok, "for the moment, only DiskFileStore is supported, but this is: %T", pm.deps.FileStore)

	if partitionID == "" {
		panic(fmt.Sprintf("partition id cannot be nil for recordPartitionTimestamp"))
	}
	if err := fs.writePartitionTimestamp(partitionID, filename, ts); err != nil {
		pm.debugf("[PARTITION] Failed to write %s for %s: %v", filename, partitionID, err)
	}
	pm.debugf("[PARTITION] Wrote %s for %s, %v", filename, partitionID, ts)
}

// errorf creates a detailed error with full stack trace and human-readable dump
func (pm *PartitionManager) errorf(received []byte, errorMessage string) error {
	// Get full stack trace
	stack := make([]byte, 4096)
	runtime.Stack(stack, false)

	// Log comprehensive debug info
	pm.logf("UNMARSHAL ERROR: %s\nStack trace:\n%s\nReceived data (%d bytes):\n%s",
		errorMessage, string(stack), len(received), string(received))

	return fmt.Errorf("%s (received %d bytes, see logs for full details)", errorMessage, len(received))
}

// verifyFileChecksum validates file content against its expected SHA-256 checksum
func (pm *PartitionManager) verifyFileChecksum(content []byte, expectedChecksum, path string, peerID types.NodeID) error {
	hash := sha256.Sum256(content)
	actualChecksum := hex.EncodeToString(hash[:])
	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch for %s from peer %s: expected %s, got %s",
			path, peerID, expectedChecksum, actualChecksum)
	}
	return nil
}

func (pm *PartitionManager) notifyFileListChanged() {
	if pm.deps.NotifyFileListChanged != nil {
		pm.deps.NotifyFileListChanged()
	}
}

func (pm *PartitionManager) loadPeer(id types.NodeID) (*types.PeerInfo, bool) {

	return pm.deps.Cluster.LoadPeer(id)
}

func (pm *PartitionManager) httpClient() *http.Client {
	if pm.deps.HttpDataClient != nil {
		return pm.deps.HttpDataClient
	}
	return http.DefaultClient
}

func (pm *PartitionManager) checkCircuitBreaker(target string) error {
	types.Assertf(pm.deps != nil, "partition manager dependencies must not be nil when checking circuit breaker for target %s", target)
	types.Assertf(pm.deps.Cluster != nil, "cluster dependency must not be nil when checking circuit breaker for target %s", target)
	return pm.deps.Cluster.CheckCircuitBreaker(target)
}

func (pm *PartitionManager) tripCircuitBreaker(target string, err error) {
	if err == nil {
		return
	}
	types.Assertf(pm.deps != nil, "partition manager dependencies must not be nil when tripping circuit breaker for target %s with error %v", target, err)
	types.Assertf(pm.deps.Cluster != nil, "cluster dependency must not be nil when tripping circuit breaker for target %s with error %v", target, err)
	pm.deps.Cluster.TripCircuitBreaker(target, err)
}

func (pm *PartitionManager) getPeers() []*types.PeerInfo {
	types.Assertf(pm.deps.Discovery != nil, "discovery must not be nil when fetching peers for node %s", pm.deps.NodeID)
	return pm.deps.Discovery.GetPeers()
}

func (pm *PartitionManager) getPeer(nodeID types.NodeID) *types.PeerInfo {
	peers := pm.getPeers()
	for _, peer := range peers {
		if peer.NodeID == nodeID {
			return peer
		}
	}
	return nil
}

func (pm *PartitionManager) hasFrogpond() bool {
	return pm.deps.Frogpond != nil
}

func (pm *PartitionManager) replicationFactor() int {

	if rf := pm.deps.Cluster.GetCurrentRF(); rf > 0 {
		return rf
	}

	return defaultReplicationFactor
}

// FIXME utility
// HashToPartition calculates which partition a filename belongs to
func HashToPartition(filename string) types.PartitionID {
	return types.PartitionIDForPath(filename)
}

// CalculatePartitionName implements the interface method
func (pm *PartitionManager) CalculatePartitionName(path string) string {
	return string(HashToPartition(path))
}

// storeFileInPartition stores a file with metadata and content in separate stores
func (pm *PartitionManager) StoreFileInPartition(ctx context.Context, path string, metadataJSON []byte, fileContent []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if pm.deps.Cluster.NoStore() {
		panic("wtf")
	}
	pm.RecordEssentialDiskActivity()
	// If in no-store mode, don't store locally
	if pm.deps.Cluster.NoStore() {
		//FIXME panic here
		pm.deps.Logger.Panicf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)

	pm.debugf("[PARTITION] Storing file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))
	if pm.addLocalPartition(partitionID) {
		pm.publishLocalPartitions()
	}

	// Store metadata in filesKV (metadata store)
	if err := pm.deps.FileStore.Put(path, metadataJSON, fileContent); err != nil {
		//pm.deps.Logger.Panicf("failed to store file: %v", err)
		return fmt.Errorf("failed to store file: %v", err)
	}

	pm.logf("[PARTITION] Stored file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))

	// Update indexer with new file
	if pm.deps.Indexer != nil {
		var metadata types.FileMetadata
		if err := json.Unmarshal(metadataJSON, &metadata); err == nil {
			types.Assertf(metadata.Path != "", "metadata path for %s must not be empty", path)
			pm.deps.Indexer.AddFile(path, metadata)
		}
	}

	// Update partition metadata in CRDT
	pm.MarkFileForSync(path, fmt.Sprintf("stored file %s", path))

	// Debug: verify what we just stored
	if metadata_bytes, _, exists, err := pm.deps.FileStore.Get(path); err == nil && exists {
		var parsedMeta types.FileMetadata
		if json.Unmarshal(metadata_bytes, &parsedMeta) == nil {
			if parsedMeta.Checksum == "" {
				panic("fuck ai")
				//pm.logf("[CHECKSUM_DEBUG] ERROR: Just stored %s but no checksum in metadata!", path)
			}
		}
	}
	return nil
}

// fetchFileStreamFromPeer streams a file from a peer without buffering it all in memory.
func (pm *PartitionManager) fetchFileStreamFromPeer(ctx context.Context, peer *types.PeerInfo, filename string) (io.ReadCloser, error) {
	decodedPath, err := url.PathUnescape(filename)
	if err != nil {
		decodedPath = filename
	}

	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, decodedPath)
	if err != nil {
		return nil, err
	}

	if err := pm.checkCircuitBreaker(fileURL); err != nil {
		return nil, err
	}

	resp, err := httpclient.Get(ctx, pm.httpClient(), fileURL,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
	)
	if err != nil {
		pm.tripCircuitBreaker(fileURL, err)
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := resp.ReadAllAndClose()
		errStatus := fmt.Errorf("peer %s returned %d for file %s via %s: %s", peer.NodeID, resp.StatusCode, filename, fileURL, strings.TrimSpace(string(body)))
		return nil, errStatus
	}

	type stream struct {
		resp *httpclient.Response
	}
	var reader stream
	reader.resp = resp

	return readCloserFunc{
		read:  func(p []byte) (int, error) { return reader.resp.Body.Read(p) },
		close: func() error { return reader.resp.Close() },
	}, nil
}

type readCloserFunc struct {
	read  func([]byte) (int, error)
	close func() error
}

func (r readCloserFunc) Read(p []byte) (int, error) { return r.read(p) }
func (r readCloserFunc) Close() error               { return r.close() }

func (pm *PartitionManager) fetchMetadataFromPeer(peer *types.PeerInfo, filename string) (types.FileMetadata, error) {
	decodedPath, err := url.PathUnescape(filename)
	types.Assertf(err == nil, "failed to unescape filename %s while fetching metadata from peer %s", filename, peer.NodeID)

	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	metadataURL, err := urlutil.BuildInternalMetadataURL(peer.Address, peer.HTTPPort, decodedPath)
	if err != nil {
		return types.FileMetadata{}, err
	}

	if err := pm.checkCircuitBreaker(metadataURL); err != nil {
		return types.FileMetadata{}, err
	}

	body, _, status, err := httpclient.SimpleGet(context.Background(), pm.httpClient(), metadataURL)
	if err != nil {
		pm.tripCircuitBreaker(metadataURL, err)
		return types.FileMetadata{}, err
	}

	if status != http.StatusOK {
		if len(body) > 0 {
			errStatus := fmt.Errorf("peer %s returned %d for metadata %s via %s: %s", peer.NodeID, status, filename, metadataURL, string(body))
			return types.FileMetadata{}, errStatus
		}
		errStatus := fmt.Errorf("peer %s returned %d for metadata %s via %s", peer.NodeID, status, filename, metadataURL)
		return types.FileMetadata{}, errStatus
	}

	var metadata types.FileMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return types.FileMetadata{}, fmt.Errorf("failed to decode metadata from peer %s: %v, response body (first 500 chars): %s", peer.NodeID, err, string(body[:min(500, len(body))]))
	}

	return metadata, nil
}

// getFileAndMetaFromPartition retrieves metadata and content from separate stores
func (pm *PartitionManager) GetFileAndMetaFromPartition(path string) ([]byte, types.FileMetadata, error) {
	pm.RecordEssentialDiskActivity()
	// If in no-store mode, always try peers first
	if pm.deps.Cluster.NoStore() {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.debugf("[PARTITION] No-store mode: getting file %s from peers", path))
	}

	partitionID := HashToPartition(path)

	// Get metadata and content atomically
	metadataBytes, contentBytes, exists, err := pm.deps.FileStore.Get(path)
	if err != nil {
		if !exists {
			pm.debugf("[PARTITION] File %s not found locally (partition %s): %v", path, partitionID, err)
			return []byte{}, types.FileMetadata{}, pm.localFileNotFound(path, partitionID, err.Error())
		}
		pm.logf("[PARTITION] Failed reading local store for %s (partition %s): %v", path, partitionID, err)
		return []byte{}, types.FileMetadata{}, fmt.Errorf("failed reading local store for %s (partition %s): %w", path, partitionID, err)
	}

	if !exists {
		pm.debugf("[PARTITION] File %s lookup returned empty record (partition %s)", path, partitionID)
		return []byte{}, types.FileMetadata{}, pm.localFileNotFound(path, partitionID, "store returned empty record")

	}

	// Parse metadata
	var metadata types.FileMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, types.FileMetadata{}, pm.errorf(metadataBytes, "corrupt file metadata")
	}

	// Check if file is marked as deleted
	if metadata.Deleted {
		pm.debugf("[PARTITION] File %s marked as deleted locally (partition %s)", path, partitionID)
		return nil, types.FileMetadata{}, pm.localFileNotFound(path, partitionID, "file is marked as deleted (tombstone present)")
	}

	// Verify checksum if available
	checksum := metadata.Checksum
	if checksum == "" {
		panic("no")
	}

	if err := pm.verifyFileChecksum(contentBytes, checksum, path, pm.deps.NodeID); err != nil {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.logf("[PARTITION] Local file corruption detected for %s: %v", path, err))
	}
	pm.debugf("[PARTITION] Local checksum verified for %s", path)

	pm.debugf("[PARTITION] Found file %s locally in partition %s", path, partitionID)
	pm.debugf("[PARTITION] Retrieved file %s: %d bytes content", path, len(contentBytes))
	return contentBytes, metadata, nil
}

// GetFileAndMetaFromPartitionStream retrieves metadata and a streaming reader when supported by the filestore.
func (pm *PartitionManager) GetFileAndMetaFromPartitionStream(path string) (io.ReadCloser, types.FileMetadata, error) {
	pm.RecordEssentialDiskActivity()
	if pm.deps.Cluster.NoStore() {
		return nil, types.FileMetadata{}, fmt.Errorf("%v", pm.debugf("[PARTITION] No-store mode: getting file %s from peers", path))
	}

	type streamFS interface {
		GetContentStream(path string) (io.ReadCloser, error)
		GetMetadata(path string) ([]byte, error)
	}

	if store, ok := pm.deps.FileStore.(streamFS); ok {
		metaBytes, err := store.GetMetadata(path)
		if err != nil {
			return nil, types.FileMetadata{}, err
		}

		var metadata types.FileMetadata
		if err := json.Unmarshal(metaBytes, &metadata); err != nil {
			return nil, types.FileMetadata{}, pm.errorf(metaBytes, "corrupt file metadata")
		}
		if metadata.Deleted {
			return nil, types.FileMetadata{}, pm.localFileNotFound(path, HashToPartition(path), "file is marked as deleted (tombstone present)")
		}
		if metadata.Checksum == "" {
			panic("no")
		}
		if metadata.ModifiedAt.IsZero() {
			panic("no")
		}

		reader, err := store.GetContentStream(path)
		if err != nil {
			return nil, types.FileMetadata{}, err
		}

		return reader, metadata, nil
	}

	// Fallback to buffered path.
	content, metadata, err := pm.GetFileAndMetaFromPartition(path)
	if err != nil {
		return nil, types.FileMetadata{}, err
	}
	return io.NopCloser(bytes.NewReader(content)), metadata, nil
}

func (pm *PartitionManager) localFileNotFound(path string, partitionID types.PartitionID, detail string) error {
	message := fmt.Sprintf("file %s is missing on holder %s (partition %s)", path, pm.deps.NodeID, partitionID)
	detail = strings.TrimSpace(detail)
	if detail != "" {
		message = fmt.Sprintf("%s: %s", message, detail)
	}
	return fmt.Errorf("%w: %s", types.ErrFileNotFound, message)
}

func (pm *PartitionManager) GetMetadataFromPartition(path string) (types.FileMetadata, error) {
	pm.RecordEssentialDiskActivity()
	//pm.debugf("Starting GetMetadataFromPartition for path %v", path)
	//defer pm.debugf("Leaving GetMetadataFromPartition for path %v", path)
	if pm.deps.Cluster.NoStore() {
		pm.debugf("[PARTITION] No-store mode: getting metadata %s from peers", path)
		return types.FileMetadata{}, fmt.Errorf("[PARTITION] No-store mode: getting metadata %s from peers", path)
	}

	metadata, err := pm.deps.FileStore.GetMetadata(path)
	if err != nil {
		// It's normal for a file not to be found locally
		pm.debugf("[PARTITION] Metadata %s not found locally: %v", path, err)
		return types.FileMetadata{}, fmt.Errorf("[PARTITION] Metadata %s not found locally: %v", path, err)
	}

	var parsedMetadata types.FileMetadata
	if err := json.Unmarshal(metadata, &parsedMetadata); err != nil {
		pm.debugf("[PARTITION] Corrupt: %v", path)
		return types.FileMetadata{}, pm.errorf(metadata, "corrupt file metadata: "+path)
	}

	if parsedMetadata.Deleted {
		pm.debugf("[PARTITION] File was deleted: %v", path)
		return types.FileMetadata{}, types.ErrFileNotFound
	}

	return parsedMetadata, nil
}

func (pm *PartitionManager) GetMetadataFromPeers(path string) (types.FileMetadata, error) {
	partitionID := HashToPartition(path)
	holders := pm.deps.Cluster.GetPartitionHolders(partitionID)
	if holders == nil {
		return types.FileMetadata{}, fmt.Errorf("partition %s not found for file %s", partitionID, path)
	}

	if len(holders) == 0 {
		return types.FileMetadata{}, fmt.Errorf("no holders registered for partition %s", partitionID)
	}

	peerLookup := pm.deps.Discovery.GetPeerMap()

	orderedPeers := make([]*types.PeerInfo, 0, len(holders))
	seen := make(map[types.NodeID]bool)
	addPeer := func(nodeID types.NodeID, peer *types.PeerInfo) {
		if peer == nil {
			return
		}
		if nodeID == pm.deps.NodeID {
			return
		}
		if seen[nodeID] {
			return
		}
		if peer.Address == "" || peer.HTTPPort == 0 {
			pm.debugf("[PARTITION] Ignoring peer %s for partition %s due to missing address/port", nodeID, partitionID)
			return
		}
		orderedPeers = append(orderedPeers, peer)
		seen[nodeID] = true
	}

	for _, holder := range holders {
		if peer, ok := peerLookup.Load(string(holder)); ok {
			addPeer(holder, peer)
			continue
		}

		if peer, ok := pm.loadPeer(holder); ok { //What stupidity is this?
			addPeer(holder, peer)
			continue
		}

		//pm.debugf("[PARTITION] Holder %s for partition %s has no reachable peer info", holder, partitionID)
	}

	if len(orderedPeers) == 0 {
		if len(peerLookup.Keys()) == 0 {
			return types.FileMetadata{}, fmt.Errorf("no peers available to retrieve partition %s", partitionID)
		}
		return types.FileMetadata{}, fmt.Errorf("no registered holders available for partition %s", partitionID)
	}

	for _, peer := range orderedPeers {
		metadata, err := pm.fetchMetadataFromPeer(peer, path)
		if err != nil {
			pm.debugf("[PARTITION] Failed metadata lookup for %s from %s: %v", path, peer.NodeID, err)
			continue
		}
		return metadata, nil
	}

	return types.FileMetadata{}, fmt.Errorf("%w: %s", types.ErrFileNotFound, path)
}

// deleteFileFromPartition removes a file from its partition
func (pm *PartitionManager) DeleteFileFromPartition(ctx context.Context, path string) error {
	return pm.DeleteFileFromPartitionWithTimestamp(ctx, path, time.Now())
}

// DeleteFileFromPartitionWithTimestamp removes a file from its partition with explicit timestamp
func (pm *PartitionManager) DeleteFileFromPartitionWithTimestamp(ctx context.Context, path string, modTime time.Time) error {
	pm.RecordEssentialDiskActivity()
	// If in no-store mode, don't delete locally (we don't have it anyway)
	if pm.deps.Cluster.NoStore() {
		//FIXME panic here
		pm.debugf("[PARTITION] No-store mode: not deleting file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)

	// Get existing metadata
	existingMetadata, _ := pm.deps.FileStore.GetMetadata(path)
	var metadata types.FileMetadata
	if existingMetadata != nil {
		// Parse existing metadata
		json.Unmarshal(existingMetadata, &metadata)
	} else {
		// File doesn't exist locally, but still create tombstone for CRDT
		pm.debugf("[PARTITION] File %s not found locally, creating tombstone anyway", path)

	}

	// Mark as deleted in metadata and set modTime
	metadata.Deleted = true
	metadata.DeletedAt = modTime
	metadata.ModifiedAt = modTime

	// Store tombstone metadata and delete content
	tombstoneJSON, _ := json.Marshal(metadata)
	if err := pm.deps.FileStore.PutMetadata(path, tombstoneJSON); err != nil {
		return err
	}

	// Update indexer to track tombstone for partition sync while removing from search results
	if pm.deps.Indexer != nil {
		if metadata.Path == "" {
			metadata.Path = path
		}
		pm.deps.Indexer.AddFile(path, metadata)
	}

	// Mark the partition for re-scan
	pm.MarkFileForSync(path, fmt.Sprintf("deleted file %s", path))

	pm.logf("[PARTITION] Marked file %s as deleted in partition %s", path, partitionID)
	return nil
}

// updatePartitionMetadata updates partition info in the CRDT
// Scans the database and counts the files, checksums them, and then updates the CRDT
func (pm *PartitionManager) updatePartitionMetadata(ctx context.Context, StartPartitionID types.PartitionID) {
	types.Assert(pm.hasFrogpond(), "[PARTITION] updatePartitionMetadata called without a frogpond")

	start := time.Now()

	// In no-store mode, don't claim to hold partitions
	if pm.deps.Cluster.NoStore() {
		//FIXME panic here
		pm.debugf("[PARTITION] No-store mode: not updating partition metadata for %s", StartPartitionID)
		return
	}

	partitionsCount := make(map[types.PartitionID]int)

	pm.debugf("[PARTITION] Starting updatePartitionMetadata for partition %s", StartPartitionID)

	// Should use FileStore to do this in a partition store aware way
	pm.deps.FileStore.ScanMetadataPartition(ctx, StartPartitionID, func(path string, metadata []byte) error {
		partitionID := types.PartitionIDForPath(path)
		if ctx.Err() != nil {
			panic(fmt.Sprintf("Context closed in updatePartitionMetadata: %v after %v seconds", ctx.Err(), time.Since(start)))
		}

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err != nil {
			pm.errorf(metadata, "corrupt metadata for partition: "+string(partitionID)+" path: "+path)
			// Parse error count as existing file
			partitionsCount[partitionID] = partitionsCount[partitionID] + 1
			return nil
		}
		// Check if file is marked as deleted
		if !parsedMetadata.Deleted {
			// File is not deleted, count it
			partitionsCount[partitionID] = partitionsCount[partitionID] + 1
		}

		return nil
	})

	numPartitions := len(partitionsCount)
	if numPartitions == 0 {
		pm.removePartitionHolder(StartPartitionID)
		pm.logf("[PARTITION] Removed %s as holder for %s because no files on disk", pm.deps.NodeID, StartPartitionID)
		return
	}

	pm.debugf("[updatePartitionMetadata] Finished scan for partition %v after %v seconds", StartPartitionID, time.Since(start))

	// Build all CRDT updates for publication
	for partitionID, count := range partitionsCount {
		if ctx.Err() != nil {
			pm.deps.Logger.Printf("[FULL_REINDEX] Context cancelled during CRDT update building")
			return
		}

		// If we have no files for this partition, remove ourselves as a holder
		if count == 0 {
			pm.removePartitionHolder(partitionID)
			pm.logf("[PARTITION] Removed %s as holder for %s because no files on disk", pm.deps.NodeID, partitionID)
			continue
		}
		pm.addLocalPartition(partitionID)

		pm.logf("[FULL_REINDEX] Added %s as holder for %s (%d files)", pm.deps.NodeID, partitionID, count)
	}

	pm.publishLocalPartitions()

	if len(partitionsCount) == 0 {
		pm.recordPartitionTimestamp(StartPartitionID, lastReindexTimestampFile, start)
	} else {
		for partitionID := range partitionsCount {
			pm.recordPartitionTimestamp(partitionID, lastReindexTimestampFile, start)
		}
	}
	pm.debugf("[updatePartitionMetadata] CRDT update for all partitions in %v finished scan after %v seconds", StartPartitionID, time.Since(start))
}

// removePartitionHolder removes this node as a holder for a partition
func (pm *PartitionManager) removePartitionHolder(partitionID types.PartitionID) {
	if pm.deps.Cluster.NoStore() {
		return // No-store nodes don't claim to hold partitions
	}

	pm.updateLocalPartitionMembership(partitionID, false)
	pm.debugf("[PARTITION] Removed %s as holder for %s (local state only)", pm.deps.NodeID, partitionID)
}

// isNodeActive checks if a node is active in the CRDT nodes/ section
func (pm *PartitionManager) isNodeActive(nodeID types.NodeID) bool {
	types.Assert(pm.hasFrogpond(), "[PARTITION] missing frogpond")

	nodeKey := fmt.Sprintf("nodes/%s", nodeID)
	dp := pm.deps.Frogpond.GetDataPoint(nodeKey)

	// If node is deleted or doesn't exist, it's not active
	if dp.Deleted || len(dp.Value) == 0 {
		return false
	}

	return true
}

// StoreFileInPartitionStream stores a file using a streaming reader when supported by the filestore.
func (pm *PartitionManager) StoreFileInPartitionStream(ctx context.Context, path string, metadataJSON []byte, content io.Reader, size int64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if pm.deps.Cluster.NoStore() {
		pm.deps.Logger.Panicf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)
	pm.debugf("[PARTITION] Streaming store of file %s in partition %s (%d bytes)", path, partitionID, size)
	if pm.addLocalPartition(partitionID) {
		pm.publishLocalPartitions()
	}
	pm.RecordEssentialDiskActivity()
	if streamer, ok := pm.deps.FileStore.(interface {
		PutStream(path string, metadata []byte, content io.Reader, size int64) error
	}); ok {
		if err := streamer.PutStream(path, metadataJSON, content, size); err != nil {
			return fmt.Errorf("failed to store file via stream: %v", err)
		}
	} else {
		buf, err := types.ReadAll(content)
		if err != nil {
			return fmt.Errorf("failed to read content for store: %v", err)
		}
		if err := pm.deps.FileStore.Put(path, metadataJSON, buf); err != nil {
			return fmt.Errorf("failed to store file: %v", err)
		}
		size = int64(len(buf))
	}

	pm.logf("[PARTITION] Stored file %s in partition %s (%d bytes)", path, partitionID, size)

	if pm.deps.Indexer != nil {
		var metadata types.FileMetadata
		if err := json.Unmarshal(metadataJSON, &metadata); err == nil {
			types.Assertf(metadata.Path != "", "metadata path for %s must not be empty", path)
			pm.deps.Indexer.AddFile(path, metadata)
		}
	}

	pm.MarkFileForSync(path, fmt.Sprintf("stored file %s", path))

	if metadataBytes, _, exists, err := pm.deps.FileStore.Get(path); err == nil && exists {
		var parsedMeta types.FileMetadata
		if json.Unmarshal(metadataBytes, &parsedMeta) == nil {
			types.Assertf(parsedMeta.Checksum != "", "checksum must not be empty for %s", path)
		}
	}

	return nil
}

// getAllPartitions returns all known partitions from CRDT using individual holder keys
func (pm *PartitionManager) getAllPartitions() []types.PartitionID {
	types.Assert(pm.hasFrogpond(), "[PARTITION] missing frogpond")
	types.Assert(pm.deps.Cluster != nil, "[PARTITION] missing cluster")

	data := pm.deps.Cluster.GetAllPartitions()
	out := make([]types.PartitionID, 0, len(data))
	for partitionID, _ := range data {
		out = append(out, partitionID)
	}
	return out

}

// VerifyStoredFileIntegrity checks the integrity of all stored files by verifying their checksums
func (pm *PartitionManager) VerifyStoredFileIntegrity() map[string]interface{} {
	if pm.deps.Cluster.NoStore() {
		return map[string]interface{}{
			"status": "skipped",
			"reason": "no-store mode",
		}
	}

	verifiedCount := 0
	corruptedFiles := []string{}
	missingChecksums := 0
	totalFiles := 0

	pm.deps.FileStore.Scan("", func(filePath string, metadata_bytes, content_bytes []byte) error {
		totalFiles++

		// Parse metadata
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadata_bytes, &metadata); err != nil {
			pm.errorf(metadata_bytes, "corrupt metadata in VerifyStoredFileIntegrity")
			return nil
		}

		// Skip deleted files
		if deleted, ok := metadata["deleted"].(bool); ok && deleted {
			return nil
		}

		// Get checksum
		checksum, ok := metadata["checksum"].(string)
		if !ok || checksum == "" {
			missingChecksums++
			return nil
		}

		// Get file content
		content := content_bytes

		// Verify checksum
		path := filePath
		if metaPath, ok := metadata["path"].(string); ok && metaPath != "" {
			path = metaPath
		}
		if path == "" {
			return nil
		}

		if err := pm.verifyFileChecksum(content, checksum, path, pm.deps.NodeID); err != nil {
			pm.logf("[INTEGRITY] Corruption detected in %s (partition %s): %v", path, types.PartitionIDForPath(path), err)
			corruptedFiles = append(corruptedFiles, path)
			return nil
		}
		verifiedCount++

		return nil
	})

	return map[string]interface{}{
		"total_files":       totalFiles,
		"verified":          verifiedCount,
		"corrupted":         len(corruptedFiles),
		"missing_checksums": missingChecksums,
		"corrupted_files":   corruptedFiles,
		"status":            "completed",
	}
}

// PartitionSyncEntry represents a single file entry for partition sync
type PartitionSyncEntry struct {
	Path     string             `json:"path"`
	Metadata types.FileMetadata `json:"metadata"`
}

// PartitionSnapshot represents a consistent point-in-time view of a partition
type PartitionSnapshot struct {
	PartitionID types.PartitionID    `json:"partition_id"`
	Timestamp   int64                `json:"timestamp"`
	Version     int64                `json:"version"`
	Entries     []PartitionSyncEntry `json:"entries"`
	Checksum    string               `json:"checksum"`
}

type partitionChecksumEntry struct {
	path     string
	metadata []byte
}

// getPartitionSyncInterval returns the partition sync interval from CRDT, or default
func (pm *PartitionManager) getPartitionSyncInterval() time.Duration {

	dp := pm.deps.Frogpond.GetDataPoint("cluster/partition_sync_interval_seconds")
	if dp.Deleted || len(dp.Value) == 0 {
		return time.Duration(types.DefaultPartitionSyncIntervalSeconds) * time.Second
	}

	var seconds int
	if err := json.Unmarshal(dp.Value, &seconds); err != nil {
		return time.Duration(types.DefaultPartitionSyncIntervalSeconds) * time.Second
	}

	if seconds < 1 {
		seconds = types.DefaultPartitionSyncIntervalSeconds
	}

	dur := time.Duration(seconds) * time.Second
	return dur
}

// getPartitionSyncPaused returns whether partition sync is paused from CRDT
func (pm *PartitionManager) getPartitionSyncPaused() bool {
	types.Assert(pm.hasFrogpond(), "[PARTITION] missing frogpond")

	dp := pm.deps.Frogpond.GetDataPoint("cluster/partition_sync_paused")
	if dp.Deleted || len(dp.Value) == 0 {
		return false
	}

	var paused bool
	if err := json.Unmarshal(dp.Value, &paused); err != nil {
		return false
	}

	return paused
}

func (pm *PartitionManager) doPartitionSync(ctx context.Context, partitionID types.PartitionID, throttle chan struct{}, holders []types.NodeID) {
	defer func() {
		if r := recover(); r != nil {
			pm.debugf("[PARTITION] Panic in partition sync for %s:  %v", partitionID, r)
			return
		}
	}()
	defer func() { <-throttle }()
	// Try all available holders for this partition
	for i, holderID := range holders {
		if ctx.Err() != nil {
			return
		}
		pm.debugf("[PARTITION] Syncing %s from %s (%v of %v)", partitionID, holderID, i+1, len(holders))

		// Find the peer in the nodes crdt
		nodeData := pm.deps.Cluster.GetNodeInfo(holderID)
		if nodeData == nil {
			//If we can't, then remove the peer as a holder, from the crdt
			pm.removePeerHolder(partitionID, holderID, 30*time.Minute)
			pm.logf("[PARTITION] Removed %s as holder for %s because not found in CRDT", holderID, partitionID)
		}
		err := pm.syncPartitionWithPeer(ctx, partitionID, holderID)
		if err != nil {
			pm.logf("[PARTITION] Failed to sync %s from %s: %v", partitionID, holderID, err)
		}
	}
}

// periodicPartitionCheck continuously syncs partitions one at a time
func (pm *PartitionManager) PeriodicSyncCheck(ctx context.Context) {
	// Skip partition syncing if in no-store mode (client mode)
	if pm.deps.Cluster.NoStore() {
		pm.debugf("[PARTITION] No-store mode: skipping partition sync")
		<-ctx.Done() // Wait until context is done i.e. shutdown
		return
	}

	throttle := make(chan struct{}, 50)
	defer close(throttle)

	// Loop forever, checking for partitions to sync
	for {
		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
			// Check if sync is paused
			if pm.getPartitionSyncPaused() {
				// Sync is paused, wait a bit before checking again
				syncInterval := pm.getPartitionSyncInterval()
				pm.debugf("Waiting syncInterval %v", syncInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(syncInterval):
					continue
				}
			}
			pm.debugf("starting partition sync check...\n")
			// Find next partition that needs syncing based on flagged list
			if partitionID, holders := pm.findFlaggedPartitionToSyncWithHolders(ctx); partitionID != "" {

				throttle <- struct{}{}
				pm.debugf("Chose partition %v to sync, partition syncs in progress: %v", partitionID, len(throttle))
				// Throttle concurrent syncs
				pm.SyncList.Store(partitionID, false) // clear flag before syncing
				pm.SyncList.Delete(partitionID)
				go pm.doPartitionSync(ctx, partitionID, throttle, holders)

			} else {
				// Nothing to sync, wait a bit before checking again
				syncInterval := pm.getPartitionSyncInterval()
				pm.debugf("Waiting syncInterval %v", syncInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(syncInterval):
				}
			}
		}

	}
}

func (pm *PartitionManager) RunUnderReplicatedMonitor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			pm.checkUnderReplicatedPartitions(ctx)
		}
	}
}

func (pm *PartitionManager) checkUnderReplicatedPartitions(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		//FIXME panic here maybe
		return
	}
	pm.logf("[REPLICATION CHECK] Checking 65536 partitions for under-replicated files")

	for partNum := 0; partNum < 65536; partNum++ {
		partitionID := types.PartitionID(fmt.Sprintf("p%05d", partNum))
		if ctx.Err() != nil {
			return
		}

		numHolders := len(pm.deps.Cluster.GetPartitionHolders(partitionID))
		if numHolders < pm.replicationFactor() && numHolders > 0 { // If there are no holders, we will never be able to sync
			pm.MarkForSync(partitionID, fmt.Sprintf("Under replicated: have %v, need %v", numHolders, pm.replicationFactor()))
		} else {
			pm.debugf("[PARTITION]  Partition %v fully replicated", partitionID)
		}
	}
	pm.logf("[REPLICATION CHECK] Finished checking 65536 partitions for under-replicated files")
}

// findFlaggedPartitionToSyncWithHolders picks a flagged partition to sync and returns its holders.
func (pm *PartitionManager) findFlaggedPartitionToSyncWithHolders(ctx context.Context) (types.PartitionID, []types.NodeID) {
	// If in no-store mode, don't sync any partitions
	if pm.deps.Cluster.NoStore() {
		//FIXME panic here maybe
		return "", nil
	}

	partitionKeys := pm.SyncList.Keys()
	if len(partitionKeys) == 0 {
		return "", nil
	}

	pm.debugf("[PARTITION] Checking %d flagged partitions for sync (RF=%d)", len(partitionKeys), pm.replicationFactor())

	// Find partitions that are under-replicated or need syncing
	for _, partitionID := range partitionKeys {
		if ctx.Err() != nil {
			return "", []types.NodeID{}
		}

		// Get available peers once check BOTH discovery AND CRDT nodes
		availablePeerIDs := pm.deps.Discovery.GetPeerMap()
		//pm.debugf("[PARTITION] Discovery peers: %v", availablePeerIDs.Keys())
		//pm.debugf("[PARTITION] Total available peer IDs: %v", availablePeerIDs)

		local, ok := pm.localPartitions.Load(partitionID)
		if ok && local {
			pm.debugf("[PARTITION] Found partition %v locally", partitionID)
			// If we have the partition, check if we have enough holders
			if len(pm.deps.Cluster.GetPartitionHolders(partitionID)) < pm.replicationFactor() {
				// If we don't, find available holders to sync from
				// Pick a random peer from the available peers
				if len(availablePeerIDs.Keys()) > 0 {
					//FIXME check for nostore here
					peerID := availablePeerIDs.Keys()[rand.Intn(len(availablePeerIDs.Keys()))]
					pm.debugf("[PARTITION] Picked random peer %s for %s", peerID, partitionID)
					// sync to a random peer
					pm.logf("[FINDFLAGGED] Found partition %v, but not enough holders, syncing to random peer %v", partitionID, peerID)
					return partitionID, []types.NodeID{types.NodeID(peerID)}
				} else {
					pm.logf("[PARTITION] Need sync, but no available holders for %s (holders: %v, available peers: %v)", partitionID, pm.deps.Cluster.GetPartitionHolders(partitionID), availablePeerIDs.Keys())
				}
			} else {
				// If we have the partition and enough holders, sync to all holders

				// Find available holders to sync from
				var availableHolders []types.NodeID
				for _, holderID := range pm.deps.Cluster.GetPartitionHolders(partitionID) {
					_, exists := availablePeerIDs.Load(string(holderID))
					if holderID != pm.deps.NodeID && exists {
						availableHolders = append(availableHolders, holderID)
					}
				}

				// if at least some holders are online, assume the rest are rebooting or something temporary
				if len(availableHolders) > 0 {
					pm.logf("[FINDFLAGGED] Found partition %v, syncing to available holders %v", partitionID, availableHolders)
					return partitionID, availableHolders
				} else {
					pm.logf("[PARTITION] No online holders for %s (holders: %v, available peers: %v)", partitionID, pm.deps.Cluster.GetPartitionHolders(partitionID), availablePeerIDs.Keys())
				}

			}

		} else {

			var availableHolders []types.NodeID
			for _, holderID := range pm.deps.Cluster.GetPartitionHolders(partitionID) {
				_, exists := availablePeerIDs.Load(string(holderID))
				if holderID != pm.deps.NodeID && exists {
					availableHolders = append(availableHolders, holderID)
				}
			}
			pm.debugf("[PARTITION] We do not have partition %v locally, downloading from %v", partitionID, availableHolders)
			return partitionID, availableHolders
		}

	}

	pm.logf("[FINDFLAGGED] No flagged partitions found to sync")
	return "", nil // Nothing to sync
}

// ScanAllFiles scans all local partition stores and calls fn for each file
func (pm *PartitionManager) ScanAllFiles(fn func(filePath string, metadata types.FileMetadata) error) error {
	start := time.Now()
	res := pm.deps.FileStore.ScanMetadata("", func(filePath string, metadataBytes []byte) error {

		// Parse metadata
		var metadata types.FileMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			message := fmt.Sprintf("Unable to parse json for file: %v, because %v, input data was %v\n", filePath, err, string(metadataBytes))
			pm.logf("%v", message)
			panic(message)
		}

		return fn(filePath, metadata)
	})
	pm.debugf("Took %v seconds to scan all files", time.Since(start).Seconds())
	return res
}

func isIn(id types.NodeID, list []types.NodeID) bool {
	for _, item := range list {
		if id == item {
			return true
		}
	}
	return false
}

// FileStore returns the underlying FileStore for direct access (used by tests)
func (pm *PartitionManager) FileStore() types.FileStoreLike {
	return pm.deps.FileStore
}

// UpdateAllLocalPartitionsMetadata scans all local partitions and updates their metadata
func (pm *PartitionManager) UpdateAllLocalPartitionsMetadata(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		pm.debugf("[PARTITION] No-store mode: skipping initial partition metadata update")
		return
	}

	// Get all partition stores directly instead of scanning every file
	partitionStores, err := pm.deps.FileStore.GetAllPartitionStores()
	if err != nil {
		pm.debugf("[PARTITION] Failed to get partition stores: %v", err)
		return
	}

	diskStore, hasDiskStore := pm.deps.FileStore.(*DiskFileStore)

	// Mark all possible partitions for reindex based on partition stores
	for _, partitionStore := range partitionStores {
		if ctx.Err() != nil {
			return
		}

		//If we are using partition stores, mark all partitions that start with this partition store ID
		//otherwise, we are getting a list of actual partitions
		if len(partitionStore) == 3 {
			// Partition store is like "p12" mark all partitions that start with this
			// Generate all possible partition IDs for this store (p12000 to p12999)
			for i := 0; i < 1000; i++ {
				partitionID := types.PartitionID(fmt.Sprintf("%s%03d", partitionStore, i))
				pm.MarkForReindex(partitionID, fmt.Sprintf("detected partition store %s", partitionStore))
			}
			continue
		}

		if hasDiskStore {
			partitionID := types.PartitionID(partitionStore)
			if partitionID == "" {
				panic("wtf")
			}

			lastUpdate, err := diskStore.readPartitionTimestamp(partitionID, lastUpdateTimestampFile)
			if err != nil {
				pm.debugf("[PARTITION] Failed to read last update timestamp for %s: %v", partitionID, err)
			}

			lastReindex, err := diskStore.readPartitionTimestamp(partitionID, lastReindexTimestampFile)
			if err != nil {
				pm.debugf("[PARTITION] Failed to read last reindex timestamp for %s: %v", partitionID, err)
				pm.MarkForReindex(partitionID, "missing last reindex timestamp")
				continue
			}

			lastSync, err := diskStore.readPartitionTimestamp(partitionID, lastSyncTimestampFile)
			if err != nil {
				pm.debugf("[PARTITION] Failed to read last sync timestamp for %s: %v", partitionID, err)
				pm.MarkForSync(partitionID, "missing last sync timestamp")
				continue
			}

			needsReindex := lastReindex.IsZero() || lastUpdate.After(lastReindex)
			needsResync := lastSync.IsZero() || lastUpdate.After(lastSync)

			if needsReindex {

				pm.MarkForReindex(partitionID, fmt.Sprintf("timestamps out of date (reindex:%v resync:%v)", needsReindex, needsResync))

			}
			if needsResync {
				pm.MarkForSync(partitionID, fmt.Sprintf("timestamps out of date (reindex:%v resync:%v)", needsReindex, needsResync))

			}
		} else {
			panic("partition stores not active, and no disk store")
		}
	}
}

func (pm *PartitionManager) GetPartitionStats() types.PartitionStatistics {
	// Count local partitions by scanning metadata store
	localPartitions := make(map[string]bool)

	allPartitions := pm.getAllPartitions()
	totalPartitions := len(allPartitions)

	underReplicated := 0
	currentRF := pm.replicationFactor()
	totalFiles := 0

	for _, partitionID := range allPartitions {
		if isIn(pm.deps.Cluster.ID(), pm.deps.Cluster.GetPartitionHolders(partitionID)) {
			localPartitions[string(partitionID)] = true
		}
		if len(pm.deps.Cluster.GetPartitionHolders(partitionID)) < currentRF {
			underReplicated++

		}
		totalFiles += 0
	}

	syncPending := pm.SyncListPendingCount()
	reindexPending := pm.ReindexListPendingCount()

	return types.PartitionStatistics{
		Local_partitions:      len(localPartitions),
		Total_partitions:      totalPartitions,
		Under_replicated:      underReplicated,
		Pending_sync:          syncPending,
		Sync_list_pending:     syncPending,
		Reindex_list_pending:  reindexPending,
		Replication_factor:    currentRF,
		Total_files:           totalFiles,
		Partition_count_limit: types.DefaultPartitionCount,
	}
}

// ListUnderReplicatedFiles returns a per-partition view of all files that are
// currently below the replication factor.
func (pm *PartitionManager) ListUnderReplicatedFiles(ctx context.Context) ([]types.UnderReplicatedPartition, error) {
	allPartitions := pm.getAllPartitions()
	if len(allPartitions) == 0 {
		return nil, nil
	}

	var result []types.UnderReplicatedPartition

	for _, partitionID := range allPartitions {
		missing := pm.replicationFactor() - len(pm.deps.Cluster.GetPartitionHolders(partitionID))
		if missing <= 0 {
			continue
		}
		if missing < 0 {
			missing = 0
		}

		holders := append([]types.NodeID(nil), pm.deps.Cluster.GetPartitionHolders(partitionID)...)
		partition := types.UnderReplicatedPartition{
			PartitionID:       partitionID,
			Holders:           holders,
			ReplicationFactor: pm.replicationFactor(),
			MissingReplicas:   missing,
		}

		if pm.deps.Cluster.NoStore() {
			partition.FilesUnavailable = true
			partition.UnavailableMessage = "node is running with no-store enabled"
			result = append(result, partition)
			continue
		}

		if !isIn(pm.deps.NodeID, holders) {
			partition.FilesUnavailable = true
			partition.UnavailableMessage = "partition is not stored locally on this node"
			result = append(result, partition)
			continue
		}

		if pm.deps.FileStore == nil {
			partition.FilesUnavailable = true
			partition.UnavailableMessage = "file store unavailable"
			result = append(result, partition)
			continue
		}

		var files []types.UnderReplicatedFile
		err := pm.deps.FileStore.ScanMetadataPartition(ctx, partitionID, func(path string, metadata []byte) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			var meta types.FileMetadata
			if err := json.Unmarshal(metadata, &meta); err != nil {
				pm.debugf("[UNDER-REP] Failed to unmarshal metadata for %s: %v", path, err)
				return nil
			}
			if meta.Deleted {
				return nil
			}

			files = append(files, types.UnderReplicatedFile{
				PartitionID:     partitionID,
				Path:            path,
				Size:            meta.Size,
				ModifiedAt:      meta.ModifiedAt,
				Holders:         holders,
				MissingReplicas: missing,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}

		sort.Slice(files, func(i, j int) bool {
			return files[i].Path < files[j].Path
		})

		partition.Files = files
		if len(files) > partition.FileCount {
			partition.FileCount = len(files)
		}

		result = append(result, partition)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PartitionID < result[j].PartitionID
	})

	return result, nil
}
