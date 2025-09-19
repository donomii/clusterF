// partitions.go - Partitioning system for scalable file storage using existing KV stores
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultPartitionCount = 65536 // 2^16 partitions
)

type PartitionID string
type PartitionVersion int64

type PartitionInfo struct {
	ID           PartitionID      `json:"id"`
	Version      PartitionVersion `json:"version"`
	LastModified int64            `json:"last_modified"`
	FileCount    int              `json:"file_count"`
	Holders      []NodeID         `json:"holders"`
}

type PartitionManager struct {
	cluster *Cluster
	mu      sync.RWMutex
}

// Initialize partition manager
func NewPartitionManager(c *Cluster) *PartitionManager {
	pm := &PartitionManager{
		cluster: c,
	}

	return pm
}

// hashToPartition calculates which partition a filename belongs to
func hashToPartition(filename string) PartitionID {
	h := crc32.ChecksumIEEE([]byte(filename))
	partitionNum := h % DefaultPartitionCount
	return PartitionID(fmt.Sprintf("p%05d", partitionNum))
}

// storeFileInPartition stores a file with metadata and content in separate stores
func (pm *PartitionManager) storeFileInPartition(path string, metadataJSON []byte, fileContent []byte) error {
	// If in no-store mode, don't store locally
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := hashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Store metadata in filesKV (metadata store)
	if err := pm.cluster.metadataKV.Put([]byte(fileKey), metadataJSON); err != nil {
		return fmt.Errorf("failed to store file metadata: %v", err)
	}

	// Store content in crdtKV (data store) using same key
	if err := pm.cluster.contentKV.Put([]byte(fileKey), fileContent); err != nil {
		return fmt.Errorf("failed to store file content: %v", err)
	}

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Stored file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))
	return nil
}

func (pm *PartitionManager) fetchFileFromPeer(peer *PeerInfo, filename string) ([]byte, error) {
	// Try to get from this peer
	decodedPath, err := url.PathUnescape(filename)
	if err != nil {
		decodedPath = filename
	}

	if !strings.HasPrefix(decodedPath, "/") {
		decodedPath = "/" + decodedPath
	}

	fullPath := "/api/files" + decodedPath

	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", peer.Address, peer.HTTPPort),
		Path:   fullPath,
	}

	resp, err := pm.cluster.httpClient.Get(u.String())
	if err != nil {
		pm.cluster.debugf("[PARTITION] Failed to get file %s from %s: %v", filename, peer.NodeID, err)
		return nil, err
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusOK {
		pm.cluster.debugf("[PARTITION] Peer %s returned %s for file %s", peer.NodeID, resp.Status, filename)
		return nil, fmt.Errorf("peer returned %s", resp.Status)
	}
	// Read content
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		pm.cluster.debugf("[PARTITION] Failed to read file %s from %s: %v", filename, peer.NodeID, err)
		return nil, err
	}
	return content, nil
}

func (pm *PartitionManager) getFileFromPartition(filename string) ([]byte, error) {
	partitionID := hashToPartition(filename)

	partition := pm.getPartitionInfo(partitionID)

	if partition == nil {
		return nil, logerrf("partition %s not found for file %s", partitionID, filename)
	}

	holders := partition.Holders
	if len(holders) == 0 {
		return nil, logerrf("no holders found for partition %s", partitionID)
	}

	peers := pm.cluster.DiscoveryManager.GetPeers()
	for _, peer := range peers {
		for _, n := range holders {
			if NodeID(peer.NodeID) == n {
				// Try to get from this peer
				content, err := pm.fetchFileFromPeer(peer, filename)
				if err == nil {
					// Successfully fetched
					return content, nil
				}

			}
		}
	}
	return nil, fmt.Errorf("failed to retrieve file %s from any holder", filename)
}

// getFileAndMetaFromPartition retrieves metadata and content from separate stores
func (pm *PartitionManager) getFileAndMetaFromPartition(path string) ([]byte, map[string]interface{}, error) {
	// If in no-store mode, always try peers first
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: getting file %s from peers", path)
		return pm.getFileFromPeers(path)
	}

	partitionID := hashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Get metadata from filesKV (metadata store)
	metadataData, err := pm.cluster.metadataKV.Get([]byte(fileKey))
	if err != nil {
		pm.cluster.debugf("[PARTITION] File %s metadata not found locally (err: %v), trying peers", path, err)
		return pm.getFileFromPeers(path)
	}

	// Parse metadata
	var metadata map[string]interface{}
	if err := json.Unmarshal(metadataData, &metadata); err != nil {
		return nil, nil, fmt.Errorf("corrupt file metadata: %v", err)
	}

	// Check if file is marked as deleted
	if deleted, ok := metadata["deleted"].(bool); ok && deleted {
		return nil, nil, ErrFileNotFound
	}

	// Get content from crdtKV (data store) using same key
	content, err := pm.cluster.contentKV.Get([]byte(fileKey))
	if err != nil {
		pm.cluster.debugf("[PARTITION] File %s content not found locally (err: %v), trying peers", path, err)
		return pm.getFileFromPeers(path)
	}

	pm.cluster.debugf("[PARTITION] Found file %s locally in partition %s", path, partitionID)
	pm.cluster.debugf("[PARTITION] Retrieved file %s: %d bytes content", path, len(content))
	return content, metadata, nil
}

// getFileFromPeers attempts to retrieve a file from peer nodes
func (pm *PartitionManager) getFileFromPeers(path string) ([]byte, map[string]interface{}, error) {
	partitionID := hashToPartition(path)
	partition := pm.getPartitionInfo(partitionID)
	if partition == nil {
		return nil, nil, fmt.Errorf("partition %s not found for file %s", partitionID, path)
	}

	if len(partition.Holders) == 0 {
		return nil, nil, fmt.Errorf("no holders registered for partition %s", partitionID)
	}

	peers := pm.cluster.DiscoveryManager.GetPeers()
	if len(peers) == 0 {
		return nil, nil, fmt.Errorf("no peers available to retrieve partition %s", partitionID)
	}

	peerLookup := make(map[NodeID]*PeerInfo, len(peers))
	for _, peer := range peers {
		peerLookup[NodeID(peer.NodeID)] = peer
	}

	orderedPeers := make([]*PeerInfo, 0, len(partition.Holders))
	for _, holder := range partition.Holders {
		if holder == pm.cluster.ID {
			continue
		}
		peer, ok := peerLookup[holder]
		if !ok {
			pm.cluster.debugf("[PARTITION] Holder %s for partition %s is not currently available", holder, partitionID)
			continue
		}
		orderedPeers = append(orderedPeers, peer)
	}

	if len(orderedPeers) == 0 {
		return nil, nil, fmt.Errorf("no registered holders available for partition %s", partitionID)
	}

	pm.cluster.debugf("[PARTITION] Trying to get file %s from partition %s holders: %v", path, partitionID, partition.Holders)
	metadataKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	for _, peer := range orderedPeers {
		pm.cluster.debugf("[PARTITION] Requesting %s from peer %s via partition-sync", path, peer.NodeID)
		url := fmt.Sprintf("http://%s:%d/api/partition-sync/%s", peer.Address, peer.HTTPPort, partitionID)
		resp, err := pm.cluster.httpClient.Get(url)
		if err != nil {
			pm.cluster.debugf("[PARTITION] Failed partition-sync request to %s: %v", peer.NodeID, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			pm.cluster.debugf("[PARTITION] Peer %s returned status %s for partition %s", peer.NodeID, resp.Status, partitionID)
			resp.Body.Close()
			continue
		}

		decoder := json.NewDecoder(resp.Body)
		var foundMetadata map[string]interface{}
		var foundContent []byte
		entryCount := 0

		for {
			var entry struct {
				Key   string `json:"key"`
				Value []byte `json:"value"`
				Store string `json:"store"`
			}

			if err := decoder.Decode(&entry); err != nil {
				if err != io.EOF {
					pm.cluster.debugf("[PARTITION] Decode error while reading partition %s from %s: %v", partitionID, peer.NodeID, err)
				}
				break
			}
			entryCount++

			if entry.Key != metadataKey {
				continue
			}

			switch entry.Store {
			case "metadata":
				if err := json.Unmarshal(entry.Value, &foundMetadata); err != nil {
					pm.cluster.debugf("[PARTITION] Failed to unmarshal metadata for %s from %s: %v", path, peer.NodeID, err)
					foundMetadata = nil
					continue
				}
				if deleted, ok := foundMetadata["deleted"].(bool); ok && deleted {
					pm.cluster.debugf("[PARTITION] Peer %s reports %s as deleted", peer.NodeID, path)
					foundMetadata = nil
					foundContent = nil
					continue
				}
			case "content":
				foundContent = entry.Value
			}

			if foundMetadata != nil && foundContent != nil {
				resp.Body.Close()
				return foundContent, foundMetadata, nil
			}
		}

		resp.Body.Close()

		if entryCount > 0 {
			pm.cluster.debugf("[PARTITION] Peer %s returned %d entries for %s but missing metadata or content", peer.NodeID, entryCount, path)
		}
	}

	return nil, nil, fmt.Errorf("%w: %s", ErrFileNotFound, path)
}

// deleteFileFromPartition removes a file from its partition
func (pm *PartitionManager) deleteFileFromPartition(path string) error {
	// If in no-store mode, don't delete locally (we don't have it anyway)
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: not deleting file %s locally", path)
		return nil
	}

	partitionID := hashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Get existing metadata
	existingMetadata, err := pm.cluster.metadataKV.Get([]byte(fileKey))
	var metadata map[string]interface{}
	if err == nil {
		// Parse existing metadata
		json.Unmarshal(existingMetadata, &metadata)
	} else {
		// File doesn't exist locally, but still create tombstone for CRDT
		pm.cluster.debugf("[PARTITION] File %s not found locally, creating tombstone anyway", path)
		metadata = make(map[string]interface{})
	}

	// Mark as deleted in metadata
	metadata["deleted"] = true
	metadata["deleted_at"] = time.Now().Unix()
	metadata["modified_at"] = time.Now().Unix()
	if version, ok := metadata["version"].(float64); ok {
		metadata["version"] = version + 1
	} else {
		metadata["version"] = float64(1)
	}

	// Store tombstone metadata
	tombstoneJSON, _ := json.Marshal(metadata)
	pm.cluster.metadataKV.Put([]byte(fileKey), tombstoneJSON)

	// Remove content from data store
	pm.cluster.contentKV.Delete([]byte(fileKey))

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Marked file %s as deleted in partition %s", path, partitionID)
	return nil
}

// updatePartitionMetadata updates partition info in the CRDT
func (pm *PartitionManager) updatePartitionMetadata(partitionID PartitionID) {
	// In no-store mode, don't claim to hold partitions
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: not updating partition metadata for %s", partitionID)
		return
	}

	// Count files in partition by scanning the existing filesKV
	fileCount := 0
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)

	// Use MapFunc correctly - it calls the function for each key-value pair
	pm.cluster.metadataKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, prefix) && strings.Contains(key, ":file:") {
			// Parse the metadata to check if it's deleted
			var metadata map[string]interface{}
			if err := json.Unmarshal(v, &metadata); err == nil {
				// Check if file is marked as deleted
				if deleted, ok := metadata["deleted"].(bool); !ok || !deleted {
					// File is not deleted, count it
					fileCount++
				}
			} else {
				// Parse error - count as existing file
				fileCount++
			}
		}
		return nil
	})

	// If we have no files for this partition, remove ourselves as a holder
	if fileCount == 0 {
		pm.removePartitionHolder(partitionID)
		return
	}

	// Update partition metadata in CRDT using individual keys per holder
	// This prevents overwriting other nodes' holder entries
	partitionKey := fmt.Sprintf("partitions/%s", partitionID)

	// Add ourselves as a holder
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.cluster.ID)
	holderData := map[string]interface{}{
		"joined_at":  time.Now().Unix(),
		"file_count": fileCount,
	}
	holderJSON, _ := json.Marshal(holderData)

	// Update file count metadata
	metadataKey := fmt.Sprintf("%s/metadata/file_count", partitionKey)
	fileCountJSON, _ := json.Marshal(fileCount)

	// Send both updates to CRDT
	updates1 := pm.cluster.frogpond.SetDataPoint(holderKey, holderJSON)
	updates2 := pm.cluster.frogpond.SetDataPoint(metadataKey, fileCountJSON)

	if pm.cluster.contentKV != nil {
		pm.cluster.contentKV.Put([]byte(holderKey), holderJSON)
		pm.cluster.contentKV.Put([]byte(metadataKey), fileCountJSON)
	}

	// Send updates to peers
	pm.cluster.sendUpdatesToPeers(updates1)
	pm.cluster.sendUpdatesToPeers(updates2)

	pm.cluster.debugf("[PARTITION] Added %s as holder for %s (%d files)", pm.cluster.ID, partitionID, fileCount)
}

// removePartitionHolder removes this node as a holder for a partition
func (pm *PartitionManager) removePartitionHolder(partitionID PartitionID) {
	if pm.cluster.NoStore {
		return // No-store nodes don't claim to hold partitions
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionID)
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.cluster.ID)

	// Remove ourselves as a holder by setting a tombstone
	updates := pm.cluster.frogpond.SetDataPoint(holderKey, nil) // nil = delete
	if pm.cluster.contentKV != nil {
		pm.cluster.contentKV.Delete([]byte(holderKey))
	}
	pm.cluster.sendUpdatesToPeers(updates)

	pm.cluster.debugf("[PARTITION] Removed %s as holder for %s", pm.cluster.ID, partitionID)
}

// getPartitionInfo retrieves partition info from CRDT using individual holder keys
func (pm *PartitionManager) getPartitionInfo(partitionID PartitionID) *PartitionInfo {
	partitionKey := fmt.Sprintf("partitions/%s", partitionID)

	// Get all holder entries for this partition
	holderPrefix := fmt.Sprintf("%s/holders/", partitionKey)
	dataPoints := pm.cluster.frogpond.GetAllMatchingPrefix(holderPrefix)

	var holders []NodeID
	var totalFiles int
	maxTimestamp := int64(0)

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Extract node ID from key
		nodeID := strings.TrimPrefix(string(dp.Key), holderPrefix)
		holders = append(holders, NodeID(nodeID))

		// Parse holder data
		var holderData map[string]interface{}
		if err := json.Unmarshal(dp.Value, &holderData); err == nil {
			if joinedAt, ok := holderData["joined_at"].(float64); ok {
				if int64(joinedAt) > maxTimestamp {
					maxTimestamp = int64(joinedAt)
				}
			}
			if fileCount, ok := holderData["file_count"].(float64); ok {
				totalFiles = int(fileCount) // Use the latest file count
			}
		}
	}

	if len(holders) == 0 {
		return nil
	}

	return &PartitionInfo{
		ID:           partitionID,
		Version:      PartitionVersion(maxTimestamp),
		LastModified: maxTimestamp,
		FileCount:    totalFiles,
		Holders:      holders,
	}
}

// getAllPartitions returns all known partitions from CRDT using individual holder keys
func (pm *PartitionManager) getAllPartitions() map[PartitionID]*PartitionInfo {
	// Get all partition holder entries
	dataPoints := pm.cluster.frogpond.GetAllMatchingPrefix("partitions/")
	partitionMap := make(map[string]map[string]interface{}) // partitionID -> nodeID -> data

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Parse key: partitions/p12345/holders/node-name
		parts := strings.Split(string(dp.Key), "/")
		if len(parts) < 4 || parts[0] != "partitions" || parts[2] != "holders" {
			continue
		}

		partitionID := parts[1]
		nodeID := parts[3]

		if partitionMap[partitionID] == nil {
			partitionMap[partitionID] = make(map[string]interface{})
		}

		var holderData map[string]interface{}
		if err := json.Unmarshal(dp.Value, &holderData); err == nil {
			partitionMap[partitionID][nodeID] = holderData
		}
	}

	// Convert to PartitionInfo objects
	result := make(map[PartitionID]*PartitionInfo)
	for partitionID, nodeData := range partitionMap {
		var holders []NodeID
		var totalFiles int
		maxTimestamp := int64(0)

		for nodeID, data := range nodeData {
			holders = append(holders, NodeID(nodeID))

			if holderData, ok := data.(map[string]interface{}); ok {
				if joinedAt, ok := holderData["joined_at"].(float64); ok {
					if int64(joinedAt) > maxTimestamp {
						maxTimestamp = int64(joinedAt)
					}
				}
				if fileCount, ok := holderData["file_count"].(float64); ok {
					totalFiles = int(fileCount)
				}
			}
		}

		result[PartitionID(partitionID)] = &PartitionInfo{
			ID:           PartitionID(partitionID),
			Version:      PartitionVersion(maxTimestamp),
			LastModified: maxTimestamp,
			FileCount:    totalFiles,
			Holders:      holders,
		}
	}

	return result
}

// syncPartitionFromPeer synchronizes a partition from a peer node using existing KV stores
func (pm *PartitionManager) syncPartitionFromPeer(partitionID PartitionID, peerID NodeID) error {
	// Find peer address
	var peerAddr string
	var peerPort int
	peers := pm.cluster.DiscoveryManager.GetPeers()
	for _, peer := range peers {
		if NodeID(peer.NodeID) == peerID {
			peerAddr = peer.Address
			peerPort = peer.HTTPPort
			break
		}
	}

	if peerAddr == "" {
		// List available peers for debugging
		availablePeers := make([]string, 0, len(peers))
		for _, peer := range peers {
			availablePeers = append(availablePeers, fmt.Sprintf("%s@%s:%d", peer.NodeID, peer.Address, peer.HTTPPort))
		}
		return fmt.Errorf("syncPartitionFromPeer: peer '%s' not found for partition '%s'. Available peers: [%s]. Discovery may be incomplete or peer left cluster", peerID, partitionID, strings.Join(availablePeers, ", "))
	}

	pm.cluster.Logger.Printf("[PARTITION] Starting sync of %s from %s", partitionID, peerID)

	// Request partition data from peer (use a longer timeout for large streams)
	url := fmt.Sprintf("http://%s:%d/api/partition-sync/%s", peerAddr, peerPort, partitionID)
	longClient := &http.Client{Timeout: 5 * time.Minute}
	resp, err := longClient.Get(url)
	if err != nil {
		return fmt.Errorf("syncPartitionFromPeer: failed to request partition sync from peer '%s' at '%s' for partition '%s': %v", peerID, url, partitionID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("syncPartitionFromPeer: peer '%s' at '%s' returned error %d '%s' for partition '%s'", peerID, url, resp.StatusCode, resp.Status, partitionID)
	}

	// Read and apply the partition data from both stores
	decoder := json.NewDecoder(resp.Body)

	syncCount := 0
	for {
		var entry struct {
			Key   string `json:"key"`
			Value []byte `json:"value"`
			Store string `json:"store"` // "metadata" or "content"
		}

		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("syncPartitionFromPeer: failed to decode sync entry from peer '%s' for partition '%s': %v", peerID, partitionID, err)
		}

		// Check if we need this data using CRDT merge rules for files only
		localValue, err := pm.cluster.metadataKV.Get([]byte(entry.Key))
		shouldUpdate := false

		if err != nil {
			// We don't have this key, add it
			shouldUpdate = true
		} else {
			// Use CRDT merge rules - only for files, ignore directories
			if strings.Contains(entry.Key, ":file:") && entry.Store == "metadata" {
				// Parse both local and remote metadata for comparison
				var remoteMetadata, localMetadata map[string]interface{}
				if json.Unmarshal(entry.Value, &remoteMetadata) == nil && json.Unmarshal(localValue, &localMetadata) == nil {
					// CRDT rule: use latest timestamp, break ties with version
					// Also handle deleted_at timestamps for tombstones
					remoteTime, _ := remoteMetadata["modified_at"].(float64)
					localTime, _ := localMetadata["modified_at"].(float64)
					remoteDeletedAt, _ := remoteMetadata["deleted_at"].(float64)
					localDeletedAt, _ := localMetadata["deleted_at"].(float64)

					// Use the latest of modified_at or deleted_at
					if remoteDeletedAt > remoteTime {
						remoteTime = remoteDeletedAt
					}
					if localDeletedAt > localTime {
						localTime = localDeletedAt
					}

					remoteVersion, _ := remoteMetadata["version"].(float64)
					localVersion, _ := localMetadata["version"].(float64)

					if remoteTime > localTime || (remoteTime == localTime && remoteVersion > localVersion) {
						shouldUpdate = true
					}
				}
			} else {
				// For content entries or non-file entries, just accept newer data
				shouldUpdate = true
			}
		}

		if shouldUpdate {
			var storeErr error
			if entry.Store == "metadata" {
				storeErr = pm.cluster.metadataKV.Put([]byte(entry.Key), entry.Value)
			} else if entry.Store == "content" {
				storeErr = pm.cluster.contentKV.Put([]byte(entry.Key), entry.Value)
			}
			if storeErr != nil {
				pm.cluster.Logger.Printf("[PARTITION] syncPartitionFromPeer: failed to store sync entry '%s' from peer '%s' for partition '%s': %v", entry.Key, peerID, partitionID, storeErr)
			} else {
				syncCount++
			}
		}
	}

	// Update our partition metadata
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Completed sync of %s from %s (%d entries updated)", partitionID, peerID, syncCount)

	// Notify frontend that file list may have changed
	pm.cluster.notifyFileListChanged()

	return nil
}

// handlePartitionSync serves partition data to requesting peers from both stores
func (pm *PartitionManager) handlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID PartitionID) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pm.cluster.debugf("[PARTITION] Serving sync data for %s", partitionID)

	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	// Stream metadata from filesKV (metadata store)
	// FIXME: This is completely fucked
	prefix := fmt.Sprintf("partition:%s:", partitionID)
	pm.cluster.metadataKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, prefix) {
			entry := struct {
				Key   string `json:"key"`
				Value []byte `json:"value"`
				Store string `json:"store"`
			}{
				Key:   key,
				Value: v,
				Store: "metadata",
			}
			return encoder.Encode(entry)
		}
		return nil
	})

	// Stream content from crdtKV (data store)
	// FIXME this is completely fucked
	pm.cluster.contentKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, prefix) {
			entry := struct {
				Key   string `json:"key"`
				Value []byte `json:"value"`
				Store string `json:"store"`
			}{
				Key:   key,
				Value: v,
				Store: "content",
			}
			return encoder.Encode(entry)
		}
		return nil
	})
}

// periodicPartitionCheck continuously syncs partitions one at a time
func (pm *PartitionManager) periodicPartitionCheck(ctx context.Context) {
	// Skip partition syncing if in no-store mode (client mode)
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: skipping partition sync")
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Find next partition that needs syncing
			if partitionID, holders := pm.findNextPartitionToSyncWithHolders(); partitionID != "" {
				// Try all available holders for this partition
				syncSuccess := false
				for _, holderID := range holders {
					pm.cluster.Logger.Printf("[PARTITION] Syncing %s from %s", partitionID, holderID)
					if err := pm.syncPartitionFromPeer(partitionID, holderID); err != nil {
						pm.cluster.Logger.Printf("[PARTITION] Failed to sync %s from %s: %v", partitionID, holderID, err)
					} else {
						syncSuccess = true
						break // Successfully synced, move to next partition
					}
				}
				if !syncSuccess {
					// Brief pause after failing all holders to avoid tight error loops
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
					}
				}
			} else {
				// Nothing to sync, wait a bit before checking again
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
			}
		}
	}
}

// findNextPartitionToSyncWithHolders finds a single partition that needs syncing and returns all available holders
func (pm *PartitionManager) findNextPartitionToSyncWithHolders() (PartitionID, []NodeID) {
	// If in no-store mode, don't sync any partitions
	if pm.cluster.NoStore {
		return "", nil
	}

	allPartitions := pm.getAllPartitions()
	currentRF := pm.cluster.getCurrentRF()

	pm.cluster.debugf("[PARTITION] Checking %d partitions for sync (RF=%d)", len(allPartitions), currentRF)

	// Get available peers once
	peers := pm.cluster.DiscoveryManager.GetPeers()
	availablePeerIDs := make(map[string]bool)
	for _, peer := range peers {
		availablePeerIDs[peer.NodeID] = true
	}

	// Find partitions that are under-replicated
	for partitionID, info := range allPartitions {

		if len(info.Holders) >= currentRF {
			continue // Already properly replicated
		}
		pm.cluster.debugf("[PARTITION] Partition %s has %d holders (need %d): %v", partitionID, len(info.Holders), currentRF, info.Holders)

		// Check if we already have this partition by scanning metadata store
		hasPartition := false
		prefix := fmt.Sprintf("partition:%s:", partitionID)
		pm.cluster.metadataKV.MapFunc(func(k, v []byte) error {
			if strings.HasPrefix(string(k), prefix) {
				hasPartition = true
				return fmt.Errorf("stop") // Break the loop
			}
			return nil
		})

		pm.cluster.debugf("[PARTITION] Partition %s: hasPartition=%v (checked prefix %s)", partitionID, hasPartition, prefix)
		if hasPartition {
			continue // We already have it
		}

		// Find all available holders to sync from (must be different nodes and currently available)
		var availableHolders []NodeID
		for _, holderID := range info.Holders {
			if holderID != pm.cluster.ID && availablePeerIDs[string(holderID)] {
				availableHolders = append(availableHolders, holderID)
			}
		}

		if len(availableHolders) == 0 {
			pm.cluster.debugf("[PARTITION] No available holders for %s (holders: %v, available peers: %v)", partitionID, info.Holders, availablePeerIDs)
			continue // No available peers to sync from
		}

		return partitionID, availableHolders
	}

	return "", nil // Nothing to sync
}

// findNextPartitionToSync finds a single partition that needs syncing (legacy function)
func (pm *PartitionManager) findNextPartitionToSync() (PartitionID, NodeID) {
	partitionID, holders := pm.findNextPartitionToSyncWithHolders()
	if partitionID != "" && len(holders) > 0 {
		return partitionID, holders[0]
	}
	return "", ""
}

// getPartitionStats returns statistics about partitions
func (pm *PartitionManager) getPartitionStats() map[string]interface{} {
	// Count local partitions by scanning metadata store
	localPartitions := make(map[string]bool)
	pm.cluster.metadataKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, "partition:") {
			parts := strings.Split(key, ":")
			if len(parts) >= 2 {
				localPartitions[parts[1]] = true
			}
		}
		return nil
	})

	allPartitions := pm.getAllPartitions()
	totalPartitions := len(allPartitions)

	underReplicated := 0
	pendingSync := 0
	currentRF := pm.cluster.getCurrentRF()

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

	return map[string]interface{}{
		"local_partitions":      len(localPartitions),
		"total_partitions":      totalPartitions,
		"under_replicated":      underReplicated,
		"pending_sync":          pendingSync,
		"replication_factor":    currentRF,
		"total_files":           totalFiles,
		"partition_count_limit": DefaultPartitionCount,
	}
}
