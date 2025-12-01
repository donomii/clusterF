// partitions.go Partitioning system for scalable file storage using existing KV stores
package partitionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/metrics"
	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/donomii/frogpond"
)

const (
	defaultReplicationFactor = 3
)

type PartitionManager struct {
	deps        *types.App
	ReindexList *syncmap.SyncMap[types.PartitionID, bool]
	SyncList    *syncmap.SyncMap[types.PartitionID, bool]
}

type PartitionVersion int64

func NewPartitionManager(deps *types.App) *PartitionManager {
	return &PartitionManager{
		deps:        deps,
		ReindexList: syncmap.NewSyncMap[types.PartitionID, bool](),
		SyncList:    syncmap.NewSyncMap[types.PartitionID, bool](),
	}
}

func (pm *PartitionManager) recordEssentialDiskActivity() {
	if pm.deps.Cluster != nil {
		pm.deps.Cluster.RecordDiskActivity(types.DiskActivityEssential)
	}
}

func (pm *PartitionManager) MarkForReindex(pId types.PartitionID, reason string) {
	pm.ReindexList.Store(pId, true)
	pm.logf("[MarkForReindex] Marked partition %v for reindex, because %s", pId, reason)
}

// MarkForSync flags a partition to be synced on the next PeriodicPartitionCheck cycle.
func (pm *PartitionManager) MarkForSync(pId types.PartitionID, reason string) {
	pm.SyncList.Store(pId, true)
	pm.logf("[MarkForSync] Marked partition %v for sync, because %s", pId, reason)
}

func (pm *PartitionManager) RunReindex(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		return
	}

	start := time.Now()
	//pm.debugf("[REINDEX] Found %v partitions to reindex: %v", pm.ReindexList.Len(), pm.ReindexList.Keys())
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
	if !pm.hasFrogpond() {
		return fmt.Errorf("no frogpond available")
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionName)
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, nodeID)

	// Create a backdated tombstone to remove this node as a holder
	tombstone := []frogpond.DataPoint{{
		Key:     []byte(holderKey),
		Value:   nil,
		Updated: backdatedTime,
		Deleted: true,
	}}

	// Apply the tombstone locally and get resulting updates
	resultingUpdates := pm.deps.Frogpond.AppendDataPoints(tombstone)

	// Send both the original tombstone and any resulting updates to peers
	pm.sendUpdates(tombstone)
	if len(resultingUpdates) > 0 {
		pm.sendUpdates(resultingUpdates)
	}

	pm.debugf("[PARTITION] Removed node %s from partition %s with backdated timestamp %s", nodeID, partitionName, backdatedTime.Format(time.RFC3339))
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

func (pm *PartitionManager) recordPartitionTimestamp(partitionID types.PartitionID, filename string, ts time.Time) {
	fs, ok := pm.deps.FileStore.(*DiskFileStore)
	if !ok || partitionID == "" {
		return
	}
	if err := fs.writePartitionTimestamp(partitionID, filename, ts); err != nil {
		pm.debugf("[PARTITION] Failed to write %s for %s: %v", filename, partitionID, err)
	}
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

func (pm *PartitionManager) sendUpdates(updates []frogpond.DataPoint) {
	if pm.deps.SendUpdatesToPeers != nil && len(updates) > 0 {
		pm.deps.SendUpdatesToPeers(updates)
	}
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

func (pm *PartitionManager) getPeers() []*types.PeerInfo {
	if pm.deps.Discovery == nil {
		return nil
	}
	return pm.deps.Discovery.GetPeers()
}

func (pm *PartitionManager) hasFrogpond() bool {
	return pm.deps.Frogpond != nil
}

func (pm *PartitionManager) replicationFactor() int {
	if pm.deps.GetCurrentRF != nil {
		if rf := pm.deps.GetCurrentRF(); rf > 0 {
			return rf
		}
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
	pm.recordEssentialDiskActivity()
	// If in no-store mode, don't store locally
	if pm.deps.NoStore {
		//FIXME panic here
		pm.deps.Logger.Panicf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)

	pm.debugf("[PARTITION] Storing file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))

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
			if metadata.Path == "" {
				metadata.Path = path
			}
			pm.deps.Indexer.AddFile(path, metadata)
		}
	}

	// Update partition metadata in CRDT
	pm.MarkForReindex(partitionID, fmt.Sprintf("stored file %s", path))
	pm.MarkForSync(partitionID, fmt.Sprintf("stored file %s", path))

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

func (pm *PartitionManager) fetchFileFromPeer(peer *types.PeerInfo, filename string) ([]byte, error) {
	// Try to get from this peer
	decodedPath, err := url.PathUnescape(filename)
	if err != nil {
		decodedPath = filename
	}

	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, decodedPath)
	if err != nil {
		pm.debugf("[PARTITION] Failed to build URL for %s on %s: %v", filename, peer.NodeID, err)
		return nil, err
	}

	// Create request with internal header to prevent recursion
	content, _, status, err := httpclient.SimpleGet(context.Background(), pm.httpClient(), fileURL,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
	)
	if err != nil {
		pm.debugf("[PARTITION] Failed to get file %s from %s: %v", filename, peer.NodeID, err)
		return nil, err
	}

	if status != http.StatusOK {
		pm.debugf("[PARTITION] Peer %s returned %d for file %s", peer.NodeID, status, filename)
		if len(content) > 0 {
			return nil, fmt.Errorf("peer %s returned %d for file %s: %s", peer.NodeID, status, filename, string(content))
		}
		return nil, fmt.Errorf("peer %s returned %d for file %s", peer.NodeID, status, filename)
	}
	return content, nil
}

func (pm *PartitionManager) fetchMetadataFromPeer(peer *types.PeerInfo, filename string) (types.FileMetadata, error) {
	decodedPath, err := url.PathUnescape(filename)
	if err != nil {
		decodedPath = filename
	}

	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	metadataURL, err := urlutil.BuildInternalMetadataURL(peer.Address, peer.HTTPPort, decodedPath)
	if err != nil {
		return types.FileMetadata{}, err
	}

	body, _, status, err := httpclient.SimpleGet(context.Background(), pm.httpClient(), metadataURL)
	if err != nil {
		return types.FileMetadata{}, err
	}

	if status != http.StatusOK {
		if len(body) > 0 {
			return types.FileMetadata{}, fmt.Errorf("peer %s returned %d for metadata %s: %s", peer.NodeID, status, filename, string(body))
		}
		return types.FileMetadata{}, fmt.Errorf("peer %s returned %d for metadata %s", peer.NodeID, status, filename)
	}

	var metadata types.FileMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return types.FileMetadata{}, fmt.Errorf("failed to decode metadata from peer %s: %v, response body (first 500 chars): %s", peer.NodeID, err, string(body[:min(500, len(body))]))
	}

	return metadata, nil
}

// getFileAndMetaFromPartition retrieves metadata and content from separate stores
func (pm *PartitionManager) GetFileAndMetaFromPartition(path string) ([]byte, types.FileMetadata, error) {
	pm.recordEssentialDiskActivity()
	// If in no-store mode, always try peers first
	if pm.deps.NoStore {
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

func (pm *PartitionManager) localFileNotFound(path string, partitionID types.PartitionID, detail string) error {
	message := fmt.Sprintf("file %s is missing on holder %s (partition %s)", path, pm.deps.NodeID, partitionID)
	detail = strings.TrimSpace(detail)
	if detail != "" {
		message = fmt.Sprintf("%s: %s", message, detail)
	}
	return fmt.Errorf("%w: %s", types.ErrFileNotFound, message)
}

// GetFileFromPeers attempts to retrieve a file from peer nodes
func (pm *PartitionManager) GetFileFromPeers(path string) ([]byte, types.FileMetadata, error) {
	partitionID := HashToPartition(path)
	partition := pm.GetPartitionInfo(partitionID)
	if partition == nil {
		return nil, types.FileMetadata{}, fmt.Errorf("partition %s not found for file %s", partitionID, path)
	}

	if len(partition.Holders) == 0 {
		return nil, types.FileMetadata{}, fmt.Errorf("no holders registered for partition %s", partitionID)
	}

	peerLookup := pm.deps.Discovery.GetPeerMap()

	orderedPeers := make([]*types.PeerInfo, 0, len(partition.Holders))
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

	for _, holder := range partition.Holders {
		if peer, ok := peerLookup.Load(string(holder)); ok {
			addPeer(holder, peer)
			continue
		}

		if peer, ok := pm.loadPeer(holder); ok {
			addPeer(holder, peer)
			continue
		}

		//pm.debugf("[PARTITION] Holder %s for partition %s has no reachable peer info", holder, partitionID)
	}

	if len(orderedPeers) == 0 {
		if peerLookup.Len() == 0 {
			return nil, types.FileMetadata{}, fmt.Errorf("no peers available to retrieve partition %s", partitionID)
		}
		return nil, types.FileMetadata{}, fmt.Errorf("no registered holders available for partition %s", partitionID)
	}

	pm.debugf("[PARTITION] Fetching %s from partition %s holders: %v", path, partitionID, partition.Holders)

	for _, peer := range orderedPeers {
		metadata, err := pm.fetchMetadataFromPeer(peer, path)
		if err != nil {
			pm.debugf("[PARTITION] Failed metadata lookup for %s from %s: %v", path, peer.NodeID, err)
			continue
		}

		content, err := pm.fetchFileFromPeer(peer, path)
		if err != nil {
			pm.debugf("[PARTITION] Failed content fetch for %s from %s: %v", path, peer.NodeID, err)
			continue
		}

		// Verify checksum if available

		if err := pm.verifyFileChecksum(content, metadata.Checksum, path, peer.NodeID); err != nil {
			pm.logf("[PARTITION] Checksum verification failed for %s from %s: %v", path, peer.NodeID, err)
			continue // Try next peer
		}
		pm.debugf("[PARTITION] Checksum verified for %s from %s", path, peer.NodeID)

		return content, metadata, nil
	}

	return nil, types.FileMetadata{}, fmt.Errorf("%w: %s", types.ErrFileNotFound, path)
}

func (pm *PartitionManager) GetMetadataFromPartition(path string) (types.FileMetadata, error) {
	pm.recordEssentialDiskActivity()
	//pm.debugf("Starting GetMetadataFromPartition for path %v", path)
	//defer pm.debugf("Leaving GetMetadataFromPartition for path %v", path)
	if pm.deps.NoStore {
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
	partition := pm.GetPartitionInfo(partitionID)
	if partition == nil {
		return types.FileMetadata{}, fmt.Errorf("partition %s not found for file %s", partitionID, path)
	}

	if len(partition.Holders) == 0 {
		return types.FileMetadata{}, fmt.Errorf("no holders registered for partition %s", partitionID)
	}

	peerLookup := pm.deps.Discovery.GetPeerMap()

	orderedPeers := make([]*types.PeerInfo, 0, len(partition.Holders))
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

	for _, holder := range partition.Holders {
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
	pm.recordEssentialDiskActivity()
	// If in no-store mode, don't delete locally (we don't have it anyway)
	if pm.deps.NoStore {
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
	pm.MarkForReindex(partitionID, fmt.Sprintf("deleted file %s", path))
	pm.MarkForSync(partitionID, fmt.Sprintf("deleted file %s", path))

	pm.logf("[PARTITION] Marked file %s as deleted in partition %s", path, partitionID)
	return nil
}

// updatePartitionMetadata updates partition info in the CRDT
// Scans the database and counts the files, checksums them, and then updates the CRDT
func (pm *PartitionManager) updatePartitionMetadata(ctx context.Context, StartPartitionID types.PartitionID) {

	if !pm.hasFrogpond() {
		return
	}

	start := time.Now()

	// In no-store mode, don't claim to hold partitions
	if pm.deps.NoStore {
		//FIXME panic here
		pm.debugf("[PARTITION] No-store mode: not updating partition metadata for %s", StartPartitionID)
		return
	}

	partitionsCount := make(map[types.PartitionID]int)
	partitionsChecksums := make(map[types.PartitionID][]string)

	pm.debugf("[PARTITION] Starting updatePartitionMetadata for partition %s", StartPartitionID)

	// Should use FileStore to do this in a partition store aware way
	pm.deps.FileStore.ScanMetadataPartition(ctx, StartPartitionID, func(path string, metadata []byte) error {
		partitionID := types.PartitionIDForPath(path)
		if ctx.Err() != nil {
			panic(fmt.Sprintf("Context closed in updatePartitionMetadata: %v after %v seconds", ctx.Err(), time.Since(start)))
		}

		checksum := sha256.Sum256(metadata)
		partitionsChecksums[partitionID] = append(partitionsChecksums[partitionID], hex.EncodeToString(checksum[:]))

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
	allUpdates := []frogpond.DataPoint{}
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

		// Update partition metadata in CRDT using individual keys per holder
		partitionKey := fmt.Sprintf("partitions/%s", partitionID)

		checksum, err := pm.CalculatePartitionChecksum(ctx, partitionID)
		if checksum == "" {
			pm.deps.Logger.Printf("[FULL_REINDEX] ERROR: Unable to calculate checksum for partition %v", partitionID)
			continue
		}
		if err != nil {
			pm.deps.Logger.Printf("[FULL_REINDEX] ERROR: Unable to calculate checksum for partition %v: %v", partitionID, err)
			continue
		}

		// Add ourselves as a holder
		holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)

		if count > 0 {
			holderData := types.HolderData{
				File_count: count,
				Checksum:   checksum,
			}
			holderJSON, _ := json.Marshal(holderData)

			// Update file count metadata
			metadataKey := fmt.Sprintf("%s/metadata/file_count", partitionKey)
			fileCountJSON, _ := json.Marshal(count)

			// Add both updates to the list
			allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(holderKey, holderJSON)...)
			allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(metadataKey, fileCountJSON)...)

			pm.logf("[FULL_REINDEX] Added %s as holder for %s (%d files)", pm.deps.NodeID, partitionID, count)
		} else {
			// Remove ourselves as a holder
			pm.removePartitionHolder(partitionID)
			pm.logf("[FULL_REINDEX] Removed %s as holder for %s", pm.deps.NodeID, partitionID)
		}
	}

	// Publish all updates to peers in one batch
	pm.sendUpdates(allUpdates)

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
	if !pm.hasFrogpond() {
		return
	}

	if pm.deps.NoStore {
		return // No-store nodes don't claim to hold partitions
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionID)
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)

	// Remove ourselves as a holder by setting a tombstone
	updates := pm.deps.Frogpond.DeleteDataPoint(holderKey, 30*time.Minute)
	pm.sendUpdates(updates)

	pm.debugf("[PARTITION] Removed %s as holder for %s", pm.deps.NodeID, partitionID)
}

// isNodeActive checks if a node is active in the CRDT nodes/ section
func (pm *PartitionManager) isNodeActive(nodeID types.NodeID) bool {
	if !pm.hasFrogpond() {
		return false
	}

	nodeKey := fmt.Sprintf("nodes/%s", nodeID)
	dp := pm.deps.Frogpond.GetDataPoint(nodeKey)

	// If node is deleted or doesn't exist, it's not active
	if dp.Deleted || len(dp.Value) == 0 {
		return false
	}

	return true
}

// GetPartitionInfo retrieves partition info from CRDT using individual holder keys
func (pm *PartitionManager) GetPartitionInfo(partitionID types.PartitionID) *types.PartitionInfo {
	if !pm.hasFrogpond() {
		return nil
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionID)

	// Get all holder entries for this partition
	holderPrefix := fmt.Sprintf("%s/holders/", partitionKey)
	dataPoints := pm.deps.Frogpond.GetAllMatchingPrefix(holderPrefix)

	var holders []types.NodeID
	var totalFiles int
	checksums := make(map[types.NodeID]string)

	holderMap := make(map[types.NodeID]types.HolderData)
	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Extract node ID from key
		nodeID := strings.TrimPrefix(string(dp.Key), holderPrefix)
		holder := types.NodeID(nodeID)

		// Check if the node is active in nodes/ section
		if !pm.isNodeActive(holder) {
			pm.debugf("[PARTITION] Holder %s for partition %s not in active nodes", holder, partitionID)
			continue
		}

		holders = append(holders, holder)

		// Parse holder data
		var holderData types.HolderData
		if err := json.Unmarshal(dp.Value, &holderData); err != nil {
			pm.errorf(dp.Value, "corrupt holder data")
			continue
		}

		fileCount := holderData.File_count
		if int(fileCount) > totalFiles {
			totalFiles = int(fileCount) // Use the highest file count
		}

		checksums[holder] = holderData.Checksum
		holderMap[holder] = holderData
	}

	if len(holders) == 0 {
		return nil
	}

	// Collect partition metadata entries
	metaPrefix := fmt.Sprintf("%s/metadata/", partitionKey)
	metaPoints := pm.deps.Frogpond.GetAllMatchingPrefix(metaPrefix)
	metadata := make(map[string]json.RawMessage)
	for _, dp := range metaPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}
		key := strings.TrimPrefix(string(dp.Key), metaPrefix)
		metadata[key] = json.RawMessage(dp.Value)
	}

	return &types.PartitionInfo{
		ID:         partitionID,
		FileCount:  totalFiles,
		Holders:    holders,
		Checksums:  checksums,
		HolderData: holderMap,
		Metadata:   metadata,
	}
}

// getAllPartitions returns all known partitions from CRDT using individual holder keys
func (pm *PartitionManager) getAllPartitions() map[types.PartitionID]*types.PartitionInfo {
	if !pm.hasFrogpond() {
		return map[types.PartitionID]*types.PartitionInfo{}
	}

	// Get all partition holder/metadata entries
	dataPoints := pm.deps.Frogpond.GetAllMatchingPrefix("partitions/")
	type crdtData struct {
		holders  map[types.NodeID]types.HolderData
		metadata map[string]json.RawMessage
	}
	partitionMap := make(map[types.PartitionID]*crdtData) // partitionID -> data

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Parse key: partitions/p12345/holders/node-name
		parts := strings.Split(string(dp.Key), "/")
		if len(parts) < 3 || parts[0] != "partitions" {
			continue
		}

		partitionID := types.PartitionID(parts[1])
		if _, ok := partitionMap[partitionID]; !ok {
			partitionMap[partitionID] = &crdtData{
				holders:  make(map[types.NodeID]types.HolderData),
				metadata: make(map[string]json.RawMessage),
			}
		}

		switch parts[2] {
		case "holders":
			if len(parts) < 4 {
				continue
			}
			nodeId := types.NodeID(parts[3])
			var data types.HolderData
			err := json.Unmarshal(dp.Value, &data)
			if err != nil {
				fmt.Printf("Error: cannot unmarshal holder data: %v", err)
			}
			partitionMap[partitionID].holders[nodeId] = data
		case "metadata":
			if len(parts) < 4 {
				continue
			}
			partitionMap[partitionID].metadata[parts[3]] = json.RawMessage(dp.Value)
		}
	}

	// Convert to PartitionInfo objects

	result := make(map[types.PartitionID]*types.PartitionInfo)
	for partitionID, crdt := range partitionMap {
		var holders []types.NodeID
		var totalFiles int
		checksums := make(map[types.NodeID]string)

		for nodeID, data := range crdt.holders {
			holders = append(holders, types.NodeID(nodeID))

			if data.File_count > totalFiles {
				totalFiles = data.File_count
			}

			checksums[types.NodeID(nodeID)] = data.Checksum

		}

		result[types.PartitionID(partitionID)] = &types.PartitionInfo{
			ID:         types.PartitionID(partitionID),
			FileCount:  totalFiles,
			Holders:    holders,
			Checksums:  checksums,
			HolderData: crdt.holders,
			Metadata:   crdt.metadata,
		}
	}

	return result
}

// VerifyStoredFileIntegrity checks the integrity of all stored files by verifying their checksums
func (pm *PartitionManager) VerifyStoredFileIntegrity() map[string]interface{} {
	if pm.deps.NoStore {
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

// calculatePartitionChecksum computes a checksum for all files in a partition
func (pm *PartitionManager) CalculatePartitionChecksum(ctx context.Context, partitionID types.PartitionID) (string, error) {
	pm.debugf("[PARTITION] Calculating checksum for partition %s", partitionID)
	defer metrics.StartGlobalTimer("partition.calculate_checksum")()
	metrics.IncrementGlobalCounter("partition.calculate_checksum.calls")

	type checksumMetadata struct {
		Path string `json:"path"` // Full path like "/docs/readme.txt"
		Size int64  `json:"size"` // Total file size in bytes

		CreatedAt  time.Time `json:"created_at"`
		ModifiedAt time.Time `json:"modified_at"`

		Checksum  string    `json:"checksum,omitempty"` // SHA-256 hash in hex format
		Deleted   bool      `json:"deleted,omitempty"`
		DeletedAt time.Time `json:"deleted_at,omitempty"`
	}

	type entry struct {
		partition types.PartitionID
		path      string
		metadata  checksumMetadata
	}

	var entries []entry

	err := pm.deps.FileStore.ScanMetadataPartition(ctx, partitionID, func(path string, metadata []byte) error {
		if ctx.Err() != nil {
			metrics.IncrementGlobalCounter("partition.calculate_checksum.errors")
			return ctx.Err()
		}

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err == nil {
			if !parsedMetadata.Deleted {
				chMeta := checksumMetadata{
					Path:       path,
					Size:       parsedMetadata.Size,
					CreatedAt:  parsedMetadata.CreatedAt,
					ModifiedAt: parsedMetadata.ModifiedAt,
					Checksum:   parsedMetadata.Checksum,
					Deleted:    parsedMetadata.Deleted,
					DeletedAt:  parsedMetadata.DeletedAt,
				}
				entries = append(entries, entry{
					partition: types.PartitionIDForPath(path),
					path:      path,
					metadata:  chMeta,
				})
			}
		}
		return nil
	})
	if err != nil {
		metrics.IncrementGlobalCounter("partition.calculate_checksum.errors")
		return "", err
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].partition == entries[j].partition {
			return entries[i].path < entries[j].path
		}
		return entries[i].partition < entries[j].partition
	})

	hash := sha256.New()
	for _, e := range entries {
		data, _ := json.Marshal(e.metadata)
		hash.Write(data)
	}

	metrics.IncrementGlobalCounter("partition.calculate_checksum.success")
	metrics.AddGlobalCounter("partition.calculate_checksum.entries", int64(len(entries)))
	return hex.EncodeToString(hash.Sum(nil)), nil
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
	if !pm.hasFrogpond() {
		return false
	}

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
	for _, holderID := range holders {
		if ctx.Err() != nil {
			return
		}
		pm.debugf("[PARTITION] Syncing %s from %s", partitionID, holderID)

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
	if pm.deps.NoStore {
		pm.debugf("[PARTITION] No-store mode: skipping partition sync")
		<-ctx.Done() // Wait until context is done i.e. shutdown
		return
	}

	throttle := make(chan struct{}, 2)
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
				pm.debugf("Partition syncs in progress: %v", len(throttle))
				// Throttle concurrent syncs
				pm.SyncList.Store(partitionID, false) // clear flag before syncing
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

// findFlaggedPartitionToSyncWithHolders picks a flagged partition to sync and returns its holders.
func (pm *PartitionManager) findFlaggedPartitionToSyncWithHolders(ctx context.Context) (types.PartitionID, []types.NodeID) {
	// If in no-store mode, don't sync any partitions
	if pm.deps.NoStore {
		//FIXME panic here maybe
		return "", nil
	}

	partitionKeys := pm.SyncList.Keys()
	if len(partitionKeys) == 0 {
		return "", nil
	}

	pm.debugf("[PARTITION] Checking %d flagged partitions for sync (RF=%d)", len(partitionKeys), pm.replicationFactor())

	ourNodeId := pm.deps.NodeID

	// Find partitions that are under-replicated or need syncing
	for _, partitionID := range partitionKeys {
		if ctx.Err() != nil {
			return "", []types.NodeID{}
		}

		// Get available peers once check BOTH discovery AND CRDT nodes
		availablePeerIDs := pm.deps.Discovery.GetPeerMap()
		//pm.debugf("[PARTITION] Discovery peers: %v", availablePeerIDs.Keys())
		//pm.debugf("[PARTITION] Total available peer IDs: %v", availablePeerIDs)

		info := pm.GetPartitionInfo(partitionID)
		if info == nil {
			pm.debugf("[PARTITION] No partition info found for %s, skipping until reindex", partitionID)
			continue
		}
		if len(info.Holders) >= pm.replicationFactor() {
			_, hasPartition := info.HolderData[ourNodeId]

			if hasPartition {

				// Find available holders to sync from
				var availableHolders []types.NodeID
				for _, holderID := range info.Holders {
					_, exists := availablePeerIDs.Load(string(holderID))
					if holderID != pm.deps.NodeID && exists {

						availableHolders = append(availableHolders, holderID)
					}
				}
				if len(availableHolders) > 0 {
					return partitionID, availableHolders
				} else {
					// Pick a random peer from the available peers
					if len(availablePeerIDs.Keys()) > 0 {
						//FIXME check for nostore here
						peerID := availablePeerIDs.Keys()[rand.Intn(len(availablePeerIDs.Keys()))]
						pm.debugf("[PARTITION] Picked random peer %s for %s", peerID, partitionID)
						return partitionID, []types.NodeID{types.NodeID(peerID)}
					}
					pm.debugf("[PARTITION] Need sync, but no available holders for %s (holders: %v, available peers: %v)", partitionID, info.Holders, availablePeerIDs.Keys())
				}

			}

			continue // Already properly replicated and in sync
		}

	}

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
	if pm.deps.NoStore {
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
		time.Sleep(500 * time.Millisecond) //Give the disk time to cool down

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
	pendingSync := 0
	currentRF := pm.replicationFactor()

	for _, info := range allPartitions {
		if isIn(pm.deps.Cluster.ID(), info.Holders) {
			localPartitions[string(info.ID)] = true
		}

	}

	for _, info := range allPartitions {

		if len(info.Holders) < currentRF {
			underReplicated++
			// Check if we need to sync this partition (we don't have it but should)
			if !localPartitions[string(info.ID)] {
				pendingSync++
			}

		}
	}

	totalFiles := 0
	for _, info := range allPartitions {
		totalFiles += info.FileCount
	}

	return types.PartitionStatistics{
		Local_partitions:      len(localPartitions),
		Total_partitions:      totalPartitions,
		Under_replicated:      underReplicated,
		Pending_sync:          pendingSync,
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

	rf := pm.replicationFactor()
	var result []types.UnderReplicatedPartition

	for _, info := range allPartitions {
		missing := rf - len(info.Holders)
		if missing <= 0 {
			continue
		}
		if missing < 0 {
			missing = 0
		}

		holders := append([]types.NodeID(nil), info.Holders...)
		partition := types.UnderReplicatedPartition{
			PartitionID:       info.ID,
			Holders:           holders,
			ReplicationFactor: rf,
			MissingReplicas:   missing,
			FileCount:         info.FileCount,
			PartitionCRDT:     info,
		}

		if pm.deps.NoStore {
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
		err := pm.deps.FileStore.ScanMetadataPartition(ctx, info.ID, func(path string, metadata []byte) error {
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
				PartitionID:     info.ID,
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
