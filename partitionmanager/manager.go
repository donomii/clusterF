// partitions.go - Partitioning system for scalable file storage using existing KV stores
package partitionmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/donomii/frogpond"
)

const (
	DefaultPartitionCount    = 65536 // 2^16 partitions
	defaultReplicationFactor = 3
)

type Dependencies struct {
	NodeID                types.NodeID
	NoStore               bool
	Logger                *log.Logger
	Debugf                func(string, ...interface{})
	FileStore             types.FileStoreLike
	HttpDataClient        *http.Client
	Discovery             types.DiscoveryManagerLike
	Cluster               types.ClusterLike
	LoadPeer              PeerLoader
	Frogpond              *frogpond.Node
	SendUpdatesToPeers    func([]frogpond.DataPoint)
	NotifyFileListChanged func()
	GetCurrentRF          func() int
	Indexer               types.IndexerLike
}

type PartitionManager struct {
	deps        Dependencies
	ReindexList *syncmap.SyncMap[types.PartitionID, bool]
}

type PartitionVersion int64

type PeerLoader func(types.NodeID) (*types.PeerInfo, bool)

func NewPartitionManager(deps Dependencies) *PartitionManager {
	return &PartitionManager{deps: deps, ReindexList: syncmap.NewSyncMap[types.PartitionID, bool]()}
}

func (pm *PartitionManager) MarkForReindex(pId types.PartitionID) {
	pm.ReindexList.Store(pId, true)
	//pm.debugf("[MarkForReindex] Marked partition %v for reindex.  List is now %v", pId, pm.ReindexList.Keys())
}

func (pm *PartitionManager) RunReindex(ctx context.Context) {
	if pm.deps.Cluster.NoStore() {
		return
	}
	if pm.getPartitionSyncPaused() {
		return
	}
	start := time.Now()
	//pm.debugf("[REINDEX] Found %v partitions to reindex: %v", pm.ReindexList.Len(), pm.ReindexList.Keys())
	count := 0

	keys := pm.ReindexList.Keys()
	for _, key := range keys {
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

// RunFullReindexAtStartup runs a full reindex at startup, scanning through the entire store,
// building the updates list, and publishing it. This is different from the incremental reindex.
func (pm *PartitionManager) RunFullReindexAtStartup(ctx context.Context) {
	if pm.deps.NoStore {
		pm.debugf("[FULL_REINDEX] No-store mode: skipping full reindex at startup")
		return
	}

	if !pm.hasFrogpond() {
		pm.debugf("[FULL_REINDEX] No frogpond available: skipping full reindex at startup")
		return
	}

	start := time.Now()
	pm.deps.Logger.Printf("[FULL_REINDEX] Starting full reindex at startup")

	// Build partition counts and checksums for all partitions by scanning the entire store
	partitionsCount := make(map[types.PartitionID]int)
	partitionsChecksum := make(map[types.PartitionID]hash.Hash)
	partitionsLastUpdate := make(map[types.PartitionID]time.Time)
	processedFiles := 0

	// Get all partition stores
	allPartitionStores, err := pm.getAllPartitionStores()
	if err != nil {
		pm.deps.Logger.Printf("[FULL_REINDEX] Failed to get partition stores: %v", err)
		return
	}

	pm.deps.Logger.Printf("[FULL_REINDEX] Found %d partition stores to scan", len(allPartitionStores))

	// Scan through all partition stores
	for _, partitionStore := range allPartitionStores {
		if ctx.Err() != nil {
			pm.deps.Logger.Printf("[FULL_REINDEX] Context cancelled during startup reindex")
			return
		}

		pm.debugf("[FULL_REINDEX] Scanning partition store %s", partitionStore)
		err := pm.deps.FileStore.ScanPartitionMetaData(partitionStore, func(key_b []byte, metadata []byte) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			partitionID := types.ExtractPartitionID(string(key_b))
			processedFiles++

			var parsedMetadata types.FileMetadata
			if err := json.Unmarshal(metadata, &parsedMetadata); err != nil {
				pm.errorf(metadata, "corrupt metadata for file: "+string(key_b))
				// Parse error - count as existing file
				partitionsCount[partitionID] = partitionsCount[partitionID] + 1
				return nil
			}

			// Check if file is marked as deleted
			if !parsedMetadata.Deleted {
				// File is not deleted, count it
				partitionsCount[partitionID] = partitionsCount[partitionID] + 1

				// Track most recent modification time for this partition
				if parsedMetadata.ModifiedAt.After(partitionsLastUpdate[partitionID]) {
					partitionsLastUpdate[partitionID] = parsedMetadata.ModifiedAt
				}
			}

			// Add to checksum
			hasher, ok := partitionsChecksum[partitionID]
			if !ok {
				hasher = sha256.New()
			}
			hasher.Write(metadata)
			partitionsChecksum[partitionID] = hasher
			return nil
		})

		if err != nil {
			pm.deps.Logger.Printf("[FULL_REINDEX] Error scanning partition store %s: %v", partitionStore, err)
			// Continue with other partitions
			continue
		}
	}

	pm.deps.Logger.Printf("[FULL_REINDEX] Processed %d files across %d partitions in %v",
		processedFiles, len(partitionsCount), time.Since(start))

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
			continue
		}

		// Update partition metadata in CRDT using individual keys per holder
		partitionKey := fmt.Sprintf("partitions/%s", partitionID)

		hasher, ok := partitionsChecksum[partitionID]
		if !ok {
			pm.deps.Logger.Printf("[FULL_REINDEX] ERROR: Unable to calculate checksum for partition %v", partitionID)
			continue
		}

		// Add ourselves as a holder
		holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)
		lastUpdate := partitionsLastUpdate[partitionID]
		// If we have files but no modification time, that's data corruption
		if count > 0 && lastUpdate.IsZero() {
			panic(fmt.Sprintf("Partition %s has %d files but no modification time - data corruption detected", partitionID, count))
		}
		if count > 0 {
			holderData := types.HolderData{
				MostRecentModifiedTime: lastUpdate,
				File_count:             count,
				Checksum:               hex.EncodeToString(hasher.Sum(nil)),
			}
			holderJSON, _ := json.Marshal(holderData)

			// Update file count metadata
			metadataKey := fmt.Sprintf("%s/metadata/file_count", partitionKey)
			fileCountJSON, _ := json.Marshal(count)

			// Add both updates to the list
			allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(holderKey, holderJSON)...)
			allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(metadataKey, fileCountJSON)...)

			pm.debugf("[FULL_REINDEX] Added %s as holder for %s (%d files)", pm.deps.NodeID, partitionID, count)
		}
	}

	// Publish all updates to peers in one batch
	pm.sendUpdates(allUpdates)

	pm.deps.Logger.Printf("[FULL_REINDEX] Completed full startup reindex: %d partitions, %d files, %d CRDT updates in %v",
		len(partitionsCount), processedFiles, len(allUpdates), time.Since(start))
}

// getAllPartitionStores returns all partition store names from the FileStore
func (pm *PartitionManager) getAllPartitionStores() ([]types.PartitionStore, error) {
	partitionSet := make(map[types.PartitionStore]struct{})
	err := pm.deps.FileStore.ScanMetadataFullKeys("", func(key string, _ []byte) error {
		storeID := types.ExtractPartitionStoreID(key)
		if storeID != "" {
			partitionSet[storeID] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	partitions := make([]types.PartitionStore, 0, len(partitionSet))
	for p := range partitionSet {
		partitions = append(partitions, p)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return partitions, nil
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
		loc := ""
		if ok {
			loc = fmt.Sprintf("%s:%d: ", file, line)
		}
		message := fmt.Sprintf(loc+format, args...)
		pm.deps.Logger.Printf("%v", message)
		return message
	}
	return ""
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
func (pm *PartitionManager) verifyFileChecksum(content []byte, expectedChecksum, path, peerID string) error {
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
	if pm.deps.LoadPeer == nil {
		return nil, false
	}
	return pm.deps.LoadPeer(id)
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
	h := crc32.ChecksumIEEE([]byte(filename))
	partitionNum := h % DefaultPartitionCount
	return types.PartitionID(fmt.Sprintf("p%05d", partitionNum))
}

// CalculatePartitionName implements the interface method
func (pm *PartitionManager) CalculatePartitionName(path string) string {
	return string(HashToPartition(path))
}

// storeFileInPartition stores a file with metadata and content in separate stores
func (pm *PartitionManager) StoreFileInPartition(ctx context.Context, path string, metadataJSON []byte, fileContent []byte) error {
	// If in no-store mode, don't store locally
	if pm.deps.NoStore {
		//FIXME panic here
		pm.debugf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	pm.debugf("[PARTITION] Storing file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))

	// Store metadata in filesKV (metadata store)
	if err := pm.deps.FileStore.Put(fileKey, metadataJSON, fileContent); err != nil {
		pm.deps.Logger.Panicf("failed to store file: %v", err)
	}

	pm.logf("[PARTITION] Stored file %s  (%d bytes)", fileKey, len(fileContent))

	// Update indexer with new file
	if pm.deps.Indexer != nil {
		var metadata types.FileMetadata
		if err := json.Unmarshal(metadataJSON, &metadata); err == nil {
			pm.deps.Indexer.AddFile(path, metadata)
		}
	}

	pm.logf("[PARTITION] Updated indexer for %v", fileKey)

	// Update partition metadata in CRDT
	pm.MarkForReindex(partitionID)
	//pm.logf("[PARTITION] Marked %v for reindex", partitionID)

	// Debug: verify what we just stored
	if storedData, err := pm.deps.FileStore.Get(fileKey); err == nil && storedData.Exists {
		var parsedMeta types.FileMetadata
		if json.Unmarshal(storedData.Metadata, &parsedMeta) == nil {
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
	resp, err := httpclient.Get(context.Background(), pm.httpClient(), fileURL,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
	)
	if err != nil {
		pm.debugf("[PARTITION] Failed to get file %s from %s: %v", filename, peer.NodeID, err)
		return nil, err
	}

	// Check response
	if resp.StatusCode != http.StatusOK {
		body, _ := resp.ReadAllAndClose()
		pm.debugf("[PARTITION] Peer %s returned %s for file %s", peer.NodeID, resp.Status, filename)
		if len(body) > 0 {
			return nil, fmt.Errorf("peer %s returned %s for file %s: %s", peer.NodeID, resp.Status, filename, string(body))
		}
		return nil, fmt.Errorf("peer %s returned %s for file %s", peer.NodeID, resp.Status, filename)
	}
	// Read content
	content, err := resp.ReadAllAndClose()
	if err != nil {
		pm.debugf("[PARTITION] Failed to read file %s from %s: %v", filename, peer.NodeID, err)
		return nil, err
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

	resp, err := httpclient.Get(context.Background(), pm.httpClient(), metadataURL)
	if err != nil {
		return types.FileMetadata{}, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := resp.ReadAllAndClose()
		if len(body) > 0 {
			return types.FileMetadata{}, fmt.Errorf("peer %s returned %s for metadata %s: %s", peer.NodeID, resp.Status, filename, string(body))
		}
		return types.FileMetadata{}, fmt.Errorf("peer %s returned %s for metadata %s", peer.NodeID, resp.Status, filename)
	}

	var metadata types.FileMetadata
	body, err := resp.ReadAllAndClose()
	if err != nil {
		return types.FileMetadata{}, fmt.Errorf("failed to read response body from peer %s: %v", peer.NodeID, err)
	}

	if err := json.Unmarshal(body, &metadata); err != nil {
		return types.FileMetadata{}, fmt.Errorf("failed to decode metadata from peer %s: %v, response body (first 500 chars): %s", peer.NodeID, err, string(body[:min(500, len(body))]))
	}

	return metadata, nil
}

// getFileAndMetaFromPartition retrieves metadata and content from separate stores
func (pm *PartitionManager) GetFileAndMetaFromPartition(path string) ([]byte, types.FileMetadata, error) {
	// If in no-store mode, always try peers first
	if pm.deps.NoStore {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.debugf("[PARTITION] No-store mode: getting file %s from peers", path))
	}

	partitionID := HashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Get metadata and content atomically
	fileData, err := pm.deps.FileStore.Get(fileKey)
	if err != nil {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.debugf("[PARTITION] File %s not found locally (err: %v), trying peers", fileKey, err))
	}

	if !fileData.Exists {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.debugf("[PARTITION] File %s not found locally", fileKey))

	}

	// Parse metadata
	var metadata types.FileMetadata
	if err := json.Unmarshal(fileData.Metadata, &metadata); err != nil {
		return nil, types.FileMetadata{}, pm.errorf(fileData.Metadata, "corrupt file metadata")
	}

	// Check if file is marked as deleted
	if metadata.Deleted {
		return nil, types.FileMetadata{}, types.ErrFileNotFound
	}

	// Verify checksum if available
	checksum := metadata.Checksum
	if checksum == "" {
		panic("no")
	}

	if err := pm.verifyFileChecksum(fileData.Content, checksum, path, string(pm.deps.NodeID)); err != nil {
		return []byte{}, types.FileMetadata{}, fmt.Errorf("%v", pm.logf("[PARTITION] Local file corruption detected for %s: %v", path, err))
	}
	pm.debugf("[PARTITION] Local checksum verified for %s", path)

	pm.debugf("[PARTITION] Found file %s locally in partition %s", path, partitionID)
	pm.debugf("[PARTITION] Retrieved file %s: %d bytes content", path, len(fileData.Content))
	return fileData.Content, metadata, nil
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
	//pm.debugf("Starting GetMetadataFromPartition for path %v", path)
	//defer pm.debugf("Leaving GetMetadataFromPartition for path %v", path)
	if pm.deps.NoStore {
		pm.debugf("[PARTITION] No-store mode: getting metadata %s from peers", path)
		return types.FileMetadata{}, fmt.Errorf("[PARTITION] No-store mode: getting metadata %s from peers", path)
	}

	partitionID := HashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	metadata, err := pm.deps.FileStore.GetMetadata(fileKey)
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

	peers := pm.getPeers()
	peerLookup := make(map[types.NodeID]*types.PeerInfo, len(peers))
	for _, peer := range peers {
		peerLookup[types.NodeID(peer.NodeID)] = peer
	}

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
		if peer, ok := peerLookup[holder]; ok {
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
		if len(peers) == 0 {
			return types.FileMetadata{}, fmt.Errorf("no peers available to retrieve partition %s", partitionID)
		}
		return types.FileMetadata{}, fmt.Errorf("no registered holders available for partition %s", partitionID)
	}

	//pm.debugf("[PARTITION] Fetching metadata %s from partition %s holders: %v", path, partitionID, partition.Holders)

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
	// If in no-store mode, don't delete locally (we don't have it anyway)
	if pm.deps.NoStore {
		//FIXME panic here
		pm.debugf("[PARTITION] No-store mode: not deleting file %s locally", path)
		return nil
	}

	partitionID := HashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Get existing metadata
	existingMetadata, _ := pm.deps.FileStore.GetMetadata(fileKey)
	var metadata types.FileMetadata
	if existingMetadata != nil {
		// Parse existing metadata
		json.Unmarshal(existingMetadata, &metadata)
	} else {
		// File doesn't exist locally, but still create tombstone for CRDT
		pm.debugf("[PARTITION] File %s not found locally, creating tombstone anyway", path)

	}

	// Mark as deleted in metadata
	metadata.Deleted = true
	metadata.DeletedAt = time.Now()
	metadata.ModifiedAt = time.Now()

	// Store tombstone metadata and delete content
	tombstoneJSON, _ := json.Marshal(metadata)
	if err := pm.deps.FileStore.PutMetadata(fileKey, tombstoneJSON); err != nil {
		return err
	}

	// Update indexer to remove file
	if pm.deps.Indexer != nil {
		pm.deps.Indexer.DeleteFile(path)
	}

	// Note: We don't delete the entry entirely, just mark as deleted

	// Mark the partition for re-scan
	pm.MarkForReindex(partitionID)

	pm.logf("[PARTITION] Marked file %s as deleted in partition %s", path, partitionID)
	return nil
}

// updatePartitionMetadata updates partition info in the CRDT
// Scans the database and counts the files, checeksums them, and then updates the CRDT
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

	// Count files in partition by scanning the existing filesKV

	partitionStore := types.PartitionStore(StartPartitionID[0:3])

	partitionsCount := make(map[types.PartitionID]int)
	partitionsChecksums := make(map[types.PartitionID][]string)
	partitionsLastUpdate := make(map[types.PartitionID]time.Time)

	// Use FileStore to scan files
	pm.deps.FileStore.ScanPartitionMetaData(partitionStore, func(key_b []byte, metadata []byte) error {
		if ctx.Err() != nil {
			panic(fmt.Sprintf("Context closed in updatePartitionMetadata: %v after %v seconds", ctx.Err(), time.Since(start)))
		}
		partitionID := types.ExtractPartitionID(string(key_b))
		_, exists := partitionsChecksums[partitionID]
		if !exists {
			pm.ReindexList.Store(partitionID, false) // Cancel pending reindexes for any partitions we index here
		}
		checksum := sha256.Sum256(metadata)
		partitionsChecksums[partitionID] = append(partitionsChecksums[partitionID], hex.EncodeToString(checksum[:]))

		var parsedMetadata types.FileMetadata
		if err := json.Unmarshal(metadata, &parsedMetadata); err != nil {
			pm.errorf(metadata, "corrupt metadata for file: "+string(key_b))
			// Parse error - count as existing file
			partitionsCount[partitionID] = partitionsCount[partitionID] + 1
			return nil
		}
		// Check if file is marked as deleted
		if !parsedMetadata.Deleted {
			// File is not deleted, count it
			partitionsCount[partitionID] = partitionsCount[partitionID] + 1

			// Track most recent modification time for this partition
			if parsedMetadata.ModifiedAt.After(partitionsLastUpdate[partitionID]) {
				partitionsLastUpdate[partitionID] = parsedMetadata.ModifiedAt
			}
		}

		return nil
	})

	pm.debugf("[updatePartitionMetadata] Finished scan for partition %v after %v seconds", StartPartitionID, time.Since(start))

	allUpdates := []frogpond.DataPoint{}
	for partitionID, count := range partitionsCount {
		// If we have no files for this partition, remove ourselves as a holder
		if count == 0 {
			pm.removePartitionHolder(partitionID)
			continue
		}

		// Update partition metadata in CRDT using individual keys per holder
		// This prevents overwriting other nodes' holder entries
		partitionKey := fmt.Sprintf("partitions/%s", partitionID)

		// Calculate partition checksum
		//partitionChecksum := pm.calculatePartitionChecksum(ctx, partitionID)

		checksums, ok := partitionsChecksums[partitionID]
		if !ok || len(checksums) == 0 {
			pm.deps.Logger.Printf("ERROR: Unable to calculate checksum for partition %v\n", partitionID)
		} else {
			sort.Strings(checksums)
			hasher := sha256.New()
			for _, cs := range checksums {
				hasher.Write([]byte(cs))
			}
			// Add ourselves as a holder
			holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)
			lastUpdate := partitionsLastUpdate[partitionID]
			// If we have files but no modification time, that's data corruption
			if partitionsCount[partitionID] > 0 && lastUpdate.IsZero() {
				panic(fmt.Sprintf("Partition %s has %d files but no modification time - data corruption detected", partitionID, partitionsCount[partitionID]))
			}
			if partitionsCount[partitionID] > 0 {
				holderData := types.HolderData{
					MostRecentModifiedTime: lastUpdate,
					File_count:             partitionsCount[partitionID],
					Checksum:               hex.EncodeToString(hasher.Sum(nil)),
				}
				holderJSON, _ := json.Marshal(holderData)

				// Update file count metadata
				metadataKey := fmt.Sprintf("%s/metadata/file_count", partitionKey)
				fileCountJSON, _ := json.Marshal(partitionsCount[partitionID])

				// Send both updates to CRDT
				allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(holderKey, holderJSON)...)
				allUpdates = append(allUpdates, pm.deps.Frogpond.SetDataPoint(metadataKey, fileCountJSON)...)

				//pm.debugf("[PARTITION] Added %s as holder for %s (%d files)", pm.deps.NodeID, partitionID, partitionsCount[partitionID])
			}

		}
		//pm.debugf("[updatePartitionMetadata] Updated CRDT metadata for partition %v", partitionID)
	}
	// Send updates to peers

	pm.sendUpdates(allUpdates)
	pm.debugf("[updatePartitionMetadata] CRDT update for all partitions in %v finished scan after %v seconds", partitionStore, time.Since(start))
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
	//pm.debugf("Searching crdt for '%v'", holderPrefix)
	dataPoints := pm.deps.Frogpond.GetAllMatchingPrefix(holderPrefix)
	//pm.debugf("Found %v matches", len(dataPoints))

	var holders []types.NodeID
	var totalFiles int
	var maxTimestamp time.Time
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
			pm.debugf("[PARTITION] Holder %s for partition %s not in active nodes, removing from holders list", holder, partitionID)
			// Remove this holder from CRDT
			updates := pm.deps.Frogpond.DeleteDataPoint(string(dp.Key), 30*time.Minute)
			pm.sendUpdates(updates)
			continue
		}

		holders = append(holders, holder)

		// Parse holder data
		var holderData types.HolderData
		if err := json.Unmarshal(dp.Value, &holderData); err != nil {
			pm.errorf(dp.Value, "corrupt holder data")
			continue
		}

		if holderData.MostRecentModifiedTime.After(maxTimestamp) {
			maxTimestamp = holderData.MostRecentModifiedTime
		}

		fileCount := holderData.File_count
		if int(fileCount) > totalFiles {
			totalFiles = int(fileCount) // Use the highest file count
		}

		checksums[holder] = holderData.Checksum

	}

	if len(holders) == 0 {
		return nil
	}

	return &types.PartitionInfo{
		ID:           partitionID,
		LastModified: maxTimestamp,
		FileCount:    totalFiles,
		Holders:      holders,
		Checksums:    checksums,
		HolderData:   holderMap,
	}
}

// getAllPartitions returns all known partitions from CRDT using individual holder keys
func (pm *PartitionManager) getAllPartitions() map[types.PartitionID]*types.PartitionInfo {
	if !pm.hasFrogpond() {
		return map[types.PartitionID]*types.PartitionInfo{}
	}

	// Get all partition holder entries
	dataPoints := pm.deps.Frogpond.GetAllMatchingPrefix("partitions/")
	partitionMap := make(map[types.PartitionID]map[types.NodeID]types.HolderData) // partitionID -> nodeID -> data

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Parse key: partitions/p12345/holders/node-name
		parts := strings.Split(string(dp.Key), "/")
		if len(parts) < 4 || parts[0] != "partitions" || parts[2] != "holders" {
			continue
		}

		partitionID := types.PartitionID(parts[1])
		nodeId := types.NodeID(parts[3])
		var data types.HolderData
		err := json.Unmarshal(dp.Value, &data)
		if err != nil {
			fmt.Printf("Error: cannot unmarshal holder data: %v", err)
		}

		_, ok := partitionMap[partitionID]
		if !ok {
			partitionMap[partitionID] = make(map[types.NodeID]types.HolderData)
		}

		partitionMap[partitionID][nodeId] = data
	}

	// Convert to PartitionInfo objects

	result := make(map[types.PartitionID]*types.PartitionInfo)
	for partitionID, nodeData := range partitionMap {
		var holders []types.NodeID
		var totalFiles int
		var maxTimestamp time.Time
		checksums := make(map[types.NodeID]string)

		for nodeID, data := range nodeData {
			holders = append(holders, types.NodeID(nodeID))

			if data.MostRecentModifiedTime.After(maxTimestamp) {
				maxTimestamp = data.MostRecentModifiedTime
			}

			if data.File_count > totalFiles {
				totalFiles = data.File_count
			}

			checksums[types.NodeID(nodeID)] = data.Checksum

		}

		result[types.PartitionID(partitionID)] = &types.PartitionInfo{
			ID:           types.PartitionID(partitionID),
			LastModified: maxTimestamp,
			FileCount:    totalFiles,
			Holders:      holders,
			Checksums:    checksums,
			HolderData:   nodeData,
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

	pm.deps.FileStore.Scan("", func(k string, metadata_bytes, content_bytes []byte) error {
		key := string(k)
		if !strings.HasPrefix(key, "partition:") || !strings.Contains(key, ":file:") {
			return nil // Skip non-file entries
		}

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
		if path, ok := metadata["path"].(string); ok {
			if err := pm.verifyFileChecksum(content, checksum, path, string(pm.deps.NodeID)); err != nil {
				pm.logf("[INTEGRITY] Corruption detected in %s: %v", path, err)
				corruptedFiles = append(corruptedFiles, path)
				return nil
			}
			verifiedCount++
		}

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
	Key      string `json:"key"`
	Metadata []byte `json:"metadata"`
	Content  []byte `json:"content"`
	Checksum string `json:"checksum"`
}

// PartitionSnapshot represents a consistent point-in-time view of a partition
type PartitionSnapshot struct {
	PartitionID types.PartitionID    `json:"partition_id"`
	Timestamp   int64                `json:"timestamp"`
	Version     int64                `json:"version"`
	Entries     []PartitionSyncEntry `json:"entries"`
	Checksum    string               `json:"checksum"`
}

// calculateEntryChecksum calculates a checksum for a single entry
func (pm *PartitionManager) calculateEntryChecksum(metadata, content []byte) string {
	hash := sha256.New()
	hash.Write(metadata)
	hash.Write(content)
	return hex.EncodeToString(hash.Sum(nil))
}

// calculatePartitionChecksum computes a checksum for all files in a partition
func (pm *PartitionManager) calculatePartitionChecksum(ctx context.Context, partitionID types.PartitionID) string {
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)
	checksum, err := pm.deps.FileStore.CalculatePartitionChecksum(ctx, prefix)
	if err != nil {
		pm.debugf("[PARTITION] Failed to calculate checksum for %s: %v", partitionID, err)
		return ""
	}
	return checksum
}

// getPartitionSyncInterval returns the partition sync interval from CRDT, or default
func (pm *PartitionManager) getPartitionSyncInterval() time.Duration {
	if !pm.hasFrogpond() {
		return 1 * time.Second
	}

	dp := pm.deps.Frogpond.GetDataPoint("cluster/partition_sync_interval_seconds")
	if dp.Deleted || len(dp.Value) == 0 {
		return 1 * time.Second
	}

	var seconds int
	if err := json.Unmarshal(dp.Value, &seconds); err != nil {
		return 1 * time.Second
	}

	if seconds < 1 {
		seconds = 1
	}

	return time.Duration(seconds) * time.Second
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
		}
		err := pm.syncPartitionWithPeer(ctx, partitionID, holderID)
		if err != nil {
			pm.logf("[PARTITION] Failed to sync %s from %s: %v", partitionID, holderID, err)
		}
	}
}

// periodicPartitionCheck continuously syncs partitions one at a time
func (pm *PartitionManager) PeriodicPartitionCheck(ctx context.Context) {
	// Skip partition syncing if in no-store mode (client mode)
	if pm.deps.NoStore {
		pm.debugf("[PARTITION] No-store mode: skipping partition sync")
		<-ctx.Done() // Wait until context is done i.e. shutdown
		return
	}

	throttle := make(chan struct{}, 6)
	defer close(throttle)

	// Loop forever, checking for partitions to sync
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Check if sync is paused
			if pm.getPartitionSyncPaused() {
				// Sync is paused, wait a bit before checking again
				syncInterval := pm.getPartitionSyncInterval()
				//pm.debugf("Waiting syncInterval %v", syncInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(syncInterval):
					continue
				}
			}
			pm.debugf("starting partition sync check...\n")
			// Find next partition that needs syncing
			if partitionID, holders := pm.findNextPartitionToSyncWithHolders(ctx); partitionID != "" {

				throttle <- struct{}{}
				// Throttle concurrent syncs
				go pm.doPartitionSync(ctx, partitionID, throttle, holders)

			} else {
				// Nothing to sync, wait a bit before checking again
				syncInterval := pm.getPartitionSyncInterval()
				//pm.debugf("Waiting syncInterval %v", syncInterval)
				select {
				case <-ctx.Done():
					return
				case <-time.After(syncInterval):
				}
			}
		}
	}
}

// findNextPartitionToSyncWithHolders finds a single partition that needs syncing and returns all available holders
func (pm *PartitionManager) findNextPartitionToSyncWithHolders(ctx context.Context) (types.PartitionID, []types.NodeID) {
	// If in no-store mode, don't sync any partitions
	if pm.deps.NoStore {
		//FIXME panic here
		return "", nil
	}

	allPartitions := pm.getAllPartitions()
	currentRF := pm.replicationFactor()

	pm.debugf("[PARTITION] Checking %d partitions for sync (RF=%d)", len(allPartitions), currentRF)

	// Get available peers once - check BOTH discovery AND CRDT nodes
	peers := pm.getPeers()
	availablePeerIDs := make(map[string]bool)
	for _, peer := range peers {
		availablePeerIDs[peer.NodeID] = true
	}
	pm.debugf("[PARTITION] Discovery peers: %v", availablePeerIDs)

	// Also add all active nodes from CRDT
	if pm.hasFrogpond() {
		nodeDataPoints := pm.deps.Frogpond.GetAllMatchingPrefix("nodes/")
		//pm.debugf("[PARTITION] Found %d node entries in CRDT", len(nodeDataPoints))
		for _, dp := range nodeDataPoints {
			//pm.debugf("[PARTITION] CRDT node key=%s deleted=%v valuelen=%d", string(dp.Key), dp.Deleted, len(dp.Value))
			if dp.Deleted || len(dp.Value) == 0 {
				continue
			}
			// Extract node ID from key: nodes/node-id
			parts := strings.Split(string(dp.Key), "/")
			if len(parts) >= 2 {
				availablePeerIDs[parts[1]] = true
				//pm.debugf("[PARTITION] Added CRDT node: %s", parts[1])
			}
		}
	}
	//pm.debugf("[PARTITION] Total available peer IDs: %v", availablePeerIDs)

	partitionKeys := make([]types.PartitionID, 0, len(allPartitions))
	for partitionID := range allPartitions {
		partitionKeys = append(partitionKeys, partitionID)
	}
	//Randomize the order to avoid always picking the same partition first
	rand.Shuffle(len(partitionKeys), func(i, j int) {
		partitionKeys[i], partitionKeys[j] = partitionKeys[j], partitionKeys[i]
	})

	ourNodeId := pm.deps.NodeID

	// Find partitions that are under-replicated or need syncing
	for _, partitionID := range partitionKeys {
		if ctx.Err() != nil {
			return "", []types.NodeID{}
		}
		info := allPartitions[partitionID]
		if len(info.Holders) >= currentRF {
			// Check if we have this partition and if our modifiedAt timestamp matches other holders
			hasPartition := false
			ourHolderData, ok := info.HolderData[ourNodeId]

			if ok {
				hasPartition = true
			}

			if hasPartition {

				// Compare with other holders' last modified
				needSync := false
				for _, holderID := range info.Holders {
					if holderID != ourNodeId && !info.LastModified.Equal(ourHolderData.MostRecentModifiedTime) {
						pm.debugf("[PARTITION] Timestamp mismatch for %s: ours %s=%s, theirs %s=%s",
							partitionID, ourNodeId, ourHolderData.MostRecentModifiedTime, holderID, info.LastModified)
						needSync = true
						break
					}
				}

				if needSync {
					// Find available holders to sync from
					var availableHolders []types.NodeID
					for _, holderID := range info.Holders {
						if holderID != pm.deps.NodeID && availablePeerIDs[string(holderID)] {
							availableHolders = append(availableHolders, holderID)
						}
					}
					if len(availableHolders) > 0 {
						return partitionID, availableHolders
					}
				}
			}

			continue // Already properly replicated and in sync
		}
		//pm.debugf("[PARTITION] Partition %s has %d holders (need %d): %v", partitionID, len(info.Holders), currentRF, info.Holders)

		/*
				// Check if we already have this partition by scanning metadata store
				hasPartition := false
				prefix := fmt.Sprintf("partition:%s:", partitionID)
				pm.deps.FileStore.ScanMetadata(prefix, func(key string, meta []byte) error {
					if strings.HasPrefix(string(key), prefix) {
						hasPartition = true
						return fmt.Errorf("stop") // Break the loop
					}
					return nil
				})


			//pm.debugf("[PARTITION] Partition %s: hasPartition=%v (checked prefix %s)", partitionID, hasPartition, prefix)
			if hasPartition {
				continue // We already have it
			}
		*/

		// Check if we already have this parition by looking in the CRDT

		partitionKey := fmt.Sprintf("partitions/%s", partitionID)
		holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)
		dps := pm.deps.Frogpond.GetAllMatchingPrefix(holderKey)
		if len(dps) > 0 {
			// We are already registered as a holder
			continue
		}

		// Find all available holders to sync from (must be different nodes and currently available)
		var availableHolders []types.NodeID
		for _, holderID := range info.Holders {
			if holderID != pm.deps.NodeID && availablePeerIDs[string(holderID)] {
				availableHolders = append(availableHolders, holderID)
			}
		}

		if len(availableHolders) == 0 {
			pm.debugf("[PARTITION] No available holders for %s (holders: %v, available peers: %v)", partitionID, info.Holders, availablePeerIDs)
			continue // No available peers to sync from
		}

		return partitionID, availableHolders
	}

	return "", nil // Nothing to sync
}

// getPartitionStats returns statistics about partitions
// ScanAllFiles scans all local partition stores and calls fn for each file
func (pm *PartitionManager) ScanAllFiles(fn func(filePath string, metadata types.FileMetadata) error) error {
	start := time.Now()
	res := pm.deps.FileStore.ScanMetadata("", func(key string, metadataBytes []byte) error {
		// Extract file path from key (format: partition:pXXXXX:file:/path)

		filePath := key

		// Parse metadata
		var metadata types.FileMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			panic("no")
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

	// Get all unique partition IDs from the local store
	pm.deps.FileStore.ScanMetadataFullKeys("", func(key string, metadata []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if key == "" {
			return nil
		}
		partitionID := types.ExtractPartitionID(key)
		if partitionID != "" {
			pm.MarkForReindex(partitionID)
		}
		return nil
	})
	pm.RunReindex(ctx)
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

		// Check that all holders are active nodes

		//FIXME
		/*
			for _, holder := range info.Holders {
				if !pm.isNodeActive(holder) {
					// Remove inactive holder from CRDT
					pm.debugf("[PARTITION] Removing inactive holder %s from partition %s", holder, info.ID)
					updates := pm.deps.Frogpond.DeleteDataPoint(fmt.Sprintf("partitions/%s/holders/%s", info.ID, holder), 30*time.Minute)
					pm.sendUpdates(updates)
				}
			}
		*/
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
		Partition_count_limit: DefaultPartitionCount,
	}
}
