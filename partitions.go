// partitions.go - Partitioning system for scalable file storage using existing KV stores
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
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

// storeFileInPartition stores a file with metadata and content together
func (pm *PartitionManager) storeFileInPartition(path string, metadataJSON []byte, fileContent []byte) error {
	// If in no-store mode, don't store locally
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: not storing file %s locally", path)
		return nil
	}

	partitionID := hashToPartition(path)

	// Store both metadata and content in a single combined structure
	combinedData := map[string]interface{}{
		"metadata": json.RawMessage(metadataJSON),
		"content":  fileContent, // Will be base64 encoded by JSON
	}
	combinedJSON, _ := json.Marshal(combinedData)

	// Store in existing filesKV using partition-prefixed keys
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	if err := pm.cluster.filesKV.Put([]byte(fileKey), combinedJSON); err != nil {
		return fmt.Errorf("failed to store file: %v", err)
	}

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Stored file %s in partition %s (%d bytes)", path, partitionID, len(fileContent))
	return nil
}

func (pm *PartitionManager) fetchFileFromPeer(peer *PeerInfo, filename string) ([]byte, error) {
	// Try to get from this peer
	url := fmt.Sprintf("http://%s:%d/api/file/%s", peer.Address, peer.HTTPPort, filename)
	resp, err := pm.cluster.httpClient.Get(url)
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

// getFileAndMetaFromPartition retrieves both metadata and content together
func (pm *PartitionManager) getFileAndMetaFromPartition(path string) ([]byte, map[string]interface{}, error) {
	// If in no-store mode, always try peers first
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: getting file %s from peers", path)
		return pm.getFileFromPeers(path)
	}

	partitionID := hashToPartition(path)
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

	// Get combined data from local KV first
	combinedData, err := pm.cluster.filesKV.Get([]byte(fileKey))
	if err == nil {
		pm.cluster.debugf("[PARTITION] Found file %s locally in partition %s", path, partitionID)
		// Parse combined data
		var combined map[string]interface{}
		if err := json.Unmarshal(combinedData, &combined); err != nil {
			return nil, nil, fmt.Errorf("corrupt file data: %v", err)
		}

		// Extract metadata
		metadataRaw, ok := combined["metadata"]
		if !ok {
			return nil, nil, fmt.Errorf("no metadata in file")
		}
		var metadata map[string]interface{}
		metadataBytes, _ := json.Marshal(metadataRaw)
		json.Unmarshal(metadataBytes, &metadata)

		// Extract content - properly decode base64 encoded content
		contentInterface, ok := combined["content"]
		if !ok {
			return nil, nil, fmt.Errorf("no content in file")
		}
		var content []byte
		switch v := contentInterface.(type) {
		case string:
			// Content is base64 encoded by JSON marshaling of []byte - decode it
			if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
				content = decoded
			} else {
				// If base64 decode fails, treat as raw string
				content = []byte(v)
			}
		case []interface{}:
			// Legacy: Array of numbers (JSON representation of byte array)
			content = make([]byte, len(v))
			for i, val := range v {
				if num, ok := val.(float64); ok {
					content[i] = byte(num)
				}
			}
		case []byte:
			// Direct byte array (should not happen in JSON but handle it)
			content = v
		default:
			// Fallback: marshal back to JSON bytes
			contentBytes, _ := json.Marshal(v)
			content = contentBytes
		}

		pm.cluster.debugf("[PARTITION] Retrieved file %s: %d bytes content, content type: %T", path, len(content), contentInterface)
		return content, metadata, nil
	}

	pm.cluster.debugf("[PARTITION] File %s not found locally (err: %v), trying peers", path, err)

	// If not found locally, try to get from peers
	return pm.getFileFromPeers(path)
}

// getFileFromPeers attempts to retrieve a file from peer nodes
func (pm *PartitionManager) getFileFromPeers(path string) ([]byte, map[string]interface{}, error) {
	partitionID := hashToPartition(path)
	partition := pm.getPartitionInfo(partitionID)

	if partition == nil {
		return nil, nil, fmt.Errorf("partition %s not found for file %s", partitionID, path)
	}

	holders := partition.Holders
	if len(holders) == 0 {
		return nil, nil, fmt.Errorf("no holders found for partition %s", partitionID)
	}

	pm.cluster.debugf("[PARTITION] Trying to get file %s from partition %s holders: %v", path, partitionID, holders)

	peers := pm.cluster.DiscoveryManager.GetPeers()
	pm.cluster.debugf("[PARTITION] Available peers: %d", len(peers))
	
	// Filter holders to only include currently available peers
	availablePeerIDs := make(map[string]bool)
	for _, peer := range peers {
		availablePeerIDs[peer.NodeID] = true
	}
	
	availableHolders := make([]NodeID, 0)
	for _, holder := range holders {
		if availablePeerIDs[string(holder)] {
			availableHolders = append(availableHolders, holder)
		}
	}
	
	if len(availableHolders) == 0 {
		pm.cluster.debugf("[PARTITION] No available holders for partition %s (holders: %v, available peers: %v)", partitionID, holders, availablePeerIDs)
		return nil, nil, fmt.Errorf("no available holders found for partition %s", partitionID)
	}
	
	pm.cluster.debugf("[PARTITION] Using available holders: %v (filtered from %v)", availableHolders, holders)
	for _, peer := range peers {
		for _, holder := range availableHolders {
			if NodeID(peer.NodeID) == holder {
				// Try to get from this peer via partition sync
				url := fmt.Sprintf("http://%s:%d/api/partition-sync/%s", peer.Address, peer.HTTPPort, partitionID)
				resp, err := pm.cluster.httpClient.Get(url)
				if err != nil {
					continue
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					continue
				}

				// Parse response to find our file
				decoder := json.NewDecoder(resp.Body)
				targetKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)

				for {
					var entry struct {
						Key   string `json:"key"`
						Value []byte `json:"value"`
					}

					if err := decoder.Decode(&entry); err != nil {
						break
					}

					if entry.Key == targetKey {
						// Found our file, parse it
						var combined map[string]interface{}
						if err := json.Unmarshal(entry.Value, &combined); err != nil {
							continue
						}

						// Extract metadata
						metadataRaw, ok := combined["metadata"]
						if !ok {
							continue
						}
						var metadata map[string]interface{}
						metadataBytes, _ := json.Marshal(metadataRaw)
						json.Unmarshal(metadataBytes, &metadata)

						// Extract content - properly decode base64
						contentInterface, ok := combined["content"]
						if !ok {
							continue
						}
						var content []byte
						switch v := contentInterface.(type) {
						case string:
							// Content is base64 encoded - decode it
							if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
								content = decoded
							} else {
								content = []byte(v)
							}
						case []interface{}:
							// Array of numbers (JSON representation of byte array)
							content = make([]byte, len(v))
							for i, val := range v {
								if num, ok := val.(float64); ok {
									content[i] = byte(num)
								}
							}
						case []byte:
							content = v
						default:
							contentBytes, _ := json.Marshal(v)
							content = contentBytes
						}

						return content, metadata, nil
					}
				}
			}
		}
	}

	return nil, nil, fmt.Errorf("file %s not found in any peer", path)
}

// deleteFileFromPartition removes a file from its partition
func (pm *PartitionManager) deleteFileFromPartition(path string) error {
	// If in no-store mode, don't delete locally (we don't have it anyway)
	if pm.cluster.NoStore {
		pm.cluster.debugf("[PARTITION] No-store mode: not deleting file %s locally", path)
		return nil
	}

	partitionID := hashToPartition(path)

	// Delete from existing filesKV
	fileKey := fmt.Sprintf("partition:%s:file:%s", partitionID, path)
	pm.cluster.filesKV.Delete([]byte(fileKey))

	// Create tombstone
	tombstoneKey := fmt.Sprintf("partition:%s:tombstone:%s", partitionID, path)
	tombstoneData := map[string]interface{}{
		"deleted_at": time.Now().Unix(),
		"path":       path,
	}
	tombstoneJSON, _ := json.Marshal(tombstoneData)
	pm.cluster.filesKV.Put([]byte(tombstoneKey), tombstoneJSON)

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Deleted file %s from partition %s", path, partitionID)
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
	files, err := pm.cluster.filesKV.MapFunc(func(k, v []byte) error { return nil })
	if err != nil {
		pm.cluster.debugf("[PARTITION] Failed to count files in %s: %v", partitionID, err)
		return
	}

	pm.cluster.debugf("[PARTITION] Scanning for files in %s with prefix %s, found %d total keys", partitionID, prefix, len(files))

	for k, _ := range files {
		key := string(k)
		if strings.HasPrefix(key, prefix) && strings.Contains(key, ":file:") {
			// Extract filename from key
			parts := strings.Split(key, ":")
			if len(parts) >= 4 {
				filename := strings.Join(parts[3:], ":")
				tombstoneKey := fmt.Sprintf("partition:%s:tombstone:%s", partitionID, filename)
				if _, err := pm.cluster.filesKV.Get([]byte(tombstoneKey)); err != nil {
					// No tombstone, count this file
					fileCount++
				}
			}
		}
	}

	// Create partition info for CRDT - merge with existing holders
	existingInfo := pm.getPartitionInfo(partitionID)
	var holders []NodeID
	if existingInfo != nil {
		pm.cluster.debugf("[PARTITION] Found existing metadata for %s: holders=%v", partitionID, existingInfo.Holders)
		// Start with existing holders
		holders = make([]NodeID, len(existingInfo.Holders))
		copy(holders, existingInfo.Holders)
		
		// Add ourselves if not already in the list
		found := false
		for _, holder := range holders {
			if holder == pm.cluster.ID {
				found = true
				break
			}
		}
		if !found {
			pm.cluster.debugf("[PARTITION] Adding %s to holders for %s", pm.cluster.ID, partitionID)
			holders = append(holders, pm.cluster.ID)
		} else {
			pm.cluster.debugf("[PARTITION] %s already in holders for %s", pm.cluster.ID, partitionID)
		}
	} else {
		pm.cluster.debugf("[PARTITION] No existing metadata for %s, creating new with holder %s", partitionID, pm.cluster.ID)
		// No existing info, we're the first holder
		holders = []NodeID{pm.cluster.ID}
	}

	partitionInfo := PartitionInfo{
		ID:           partitionID,
		Version:      PartitionVersion(time.Now().UnixNano()),
		LastModified: time.Now().Unix(),
		FileCount:    fileCount,
		Holders:      holders, // Merged holder list
	}

	partitionJSON, _ := json.Marshal(partitionInfo)
	partitionKey := fmt.Sprintf("partitions/%s", partitionID)

	updates := pm.cluster.frogpond.SetDataPoint(partitionKey, partitionJSON)
	if pm.cluster.crdtKV != nil {
		pm.cluster.crdtKV.Put([]byte(partitionKey), partitionJSON)
	}
	pm.cluster.sendUpdatesToPeers(updates)

	pm.cluster.debugf("[PARTITION] Updated metadata for %s: %d files, holders: %v", partitionID, fileCount, holders)
}

// getPartitionInfo retrieves partition info from CRDT
func (pm *PartitionManager) getPartitionInfo(partitionID PartitionID) *PartitionInfo {
	partitionKey := fmt.Sprintf("partitions/%s", partitionID)
	data := pm.cluster.frogpond.GetDataPoint(partitionKey)

	if data.Deleted || len(data.Value) == 0 {
		return nil
	}

	var partitionInfo PartitionInfo
	// Make a defensive copy before unmarshalling in case the underlying buffer is reused
	safe := append([]byte(nil), data.Value...)
	if err := json.Unmarshal(safe, &partitionInfo); err != nil {
		return nil
	}

	return &partitionInfo
}

// getAllPartitions returns all known partitions from CRDT
func (pm *PartitionManager) getAllPartitions() map[PartitionID]*PartitionInfo {
	dataPoints := pm.cluster.frogpond.GetAllMatchingPrefix("partitions/")
	partitions := make(map[PartitionID]*PartitionInfo)

	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		var partitionInfo PartitionInfo
		// Defensive copy to avoid unmarshalling over a buffer that may be mutated concurrently
		safe := append([]byte{}, dp.Value...)
		if err := json.Unmarshal(safe, &partitionInfo); err != nil {
			continue
		}

		partitions[partitionInfo.ID] = &partitionInfo
	}

	return partitions
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

	// Read and apply the partition data to existing filesKV
	decoder := json.NewDecoder(resp.Body)

	syncCount := 0
	for {
		var entry struct {
			Key   string `json:"key"`
			Value []byte `json:"value"`
		}

		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("syncPartitionFromPeer: failed to decode sync entry from peer '%s' for partition '%s': %v", peerID, partitionID, err)
		}

		// Check if we need this data (simple timestamp-based sync)
		localValue, err := pm.cluster.filesKV.Get([]byte(entry.Key))
		shouldUpdate := false

		if err != nil {
			// We don't have this key, add it
			shouldUpdate = true
		} else {
			// Compare timestamps if this is metadata
			if strings.Contains(entry.Key, ":metadata") {
				var remoteMetadata, localMetadata map[string]interface{}
				json.Unmarshal(entry.Value, &remoteMetadata)
				json.Unmarshal(localValue, &localMetadata)

				remoteTime, _ := remoteMetadata["modified_at"].(float64)
				localTime, _ := localMetadata["modified_at"].(float64)

				if remoteTime > localTime {
					shouldUpdate = true
				}
			}
		}

		if shouldUpdate {
			if err := pm.cluster.filesKV.Put([]byte(entry.Key), entry.Value); err != nil {
				pm.cluster.Logger.Printf("[PARTITION] syncPartitionFromPeer: failed to store sync entry '%s' from peer '%s' for partition '%s': %v", entry.Key, peerID, partitionID, err)
			} else {
				syncCount++
			}
		}
	}

	// Update our partition metadata
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Completed sync of %s from %s (%d entries updated)", partitionID, peerID, syncCount)
	return nil
}

// handlePartitionSync serves partition data to requesting peers using existing KV stores
func (pm *PartitionManager) handlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID PartitionID) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pm.cluster.Logger.Printf("[PARTITION] Serving sync data for %s", partitionID)

	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)

	// Stream all key-value pairs for this partition from existing filesKV
	prefix := fmt.Sprintf("partition:%s:", partitionID)
	pm.cluster.filesKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, prefix) {
			entry := struct {
				Key   string `json:"key"`
				Value []byte `json:"value"`
			}{
				Key:   key,
				Value: v,
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
		pm.cluster.debugf("[PARTITION] Partition %s has %d holders (need %d): %v", partitionID, len(info.Holders), currentRF, info.Holders)
		if len(info.Holders) >= currentRF {
			continue // Already properly replicated
		}

		// Check if we already have this partition
		hasPartition := false
		prefix := fmt.Sprintf("partition:%s:", partitionID)
		pm.cluster.filesKV.MapFunc(func(k, v []byte) error {
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
	// Count local partitions by scanning existing filesKV
	localPartitions := make(map[string]bool)
	pm.cluster.filesKV.MapFunc(func(k, v []byte) error {
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
