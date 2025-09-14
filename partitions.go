// partitions.go - Partitioning system for scalable file storage using existing KV stores
package main

import (
	"context"
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

// storeFileInPartition stores a file in its appropriate partition using existing KV store
func (pm *PartitionManager) storeFileInPartition(filename string, content []byte, contentType string) error {
	partitionID := hashToPartition(filename)

	// Create file metadata
	metadata := map[string]interface{}{
		"filename":     filename,
		"content_type": contentType,
		"size":         len(content),
		"modified_at":  time.Now().Unix(),
	}
	metadataJSON, _ := json.Marshal(metadata)

	// Store in existing filesKV using partition-prefixed keys
	contentKey := fmt.Sprintf("partition:%s:file:%s:content", partitionID, filename)
	metadataKey := fmt.Sprintf("partition:%s:file:%s:metadata", partitionID, filename)

	if err := pm.cluster.filesKV.Put([]byte(contentKey), content); err != nil {
		return fmt.Errorf("failed to store file content: %v", err)
	}

	if err := pm.cluster.filesKV.Put([]byte(metadataKey), metadataJSON); err != nil {
		return fmt.Errorf("failed to store file metadata: %v", err)
	}

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Stored file %s in partition %s (%d bytes)", filename, partitionID, len(content))
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
		return nil, fmt.Errorf("partition %s not found for file %s", partitionID, filename)
	}

	holders := partition.Holders
	if len(holders) == 0 {
		return nil, fmt.Errorf("no holders found for partition %s", partitionID)
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

// getFileFromPartition retrieves a file from its partition using existing KV store
func (pm *PartitionManager) getFileAndMetaFromPartition(filename string) ([]byte, map[string]interface{}, error) {
	metadataPath := filename + ".metadata"
	fileContent, err := pm.getFileFromPartition(filename)
	if err != nil {
		return nil, nil, err
	}

	metaDataBytes, err := pm.getFileFromPartition(metadataPath)
	if err != nil {
		return nil, nil, err
	}

	metaData := make(map[string]interface{})
	json.Unmarshal(metaDataBytes, &metaData)

	return fileContent, metaData, nil
}

// deleteFileFromPartition removes a file from its partition using existing KV store
func (pm *PartitionManager) deleteFileFromPartition(filename string) error {
	partitionID := hashToPartition(filename)

	// Delete from existing filesKV using partition-prefixed keys
	contentKey := fmt.Sprintf("partition:%s:file:%s:content", partitionID, filename)
	metadataKey := fmt.Sprintf("partition:%s:file:%s:metadata", partitionID, filename)

	pm.cluster.filesKV.Delete([]byte(contentKey))
	pm.cluster.filesKV.Delete([]byte(metadataKey))

	// Create tombstone
	tombstoneKey := fmt.Sprintf("partition:%s:file:%s:tombstone", partitionID, filename)
	tombstoneData := map[string]interface{}{
		"deleted_at": time.Now().Unix(),
		"filename":   filename,
	}
	tombstoneJSON, _ := json.Marshal(tombstoneData)
	pm.cluster.filesKV.Put([]byte(tombstoneKey), tombstoneJSON)

	// Update partition metadata in CRDT
	pm.updatePartitionMetadata(partitionID)

	pm.cluster.Logger.Printf("[PARTITION] Deleted file %s from partition %s", filename, partitionID)
	return nil
}

// updatePartitionMetadata updates partition info in the CRDT
func (pm *PartitionManager) updatePartitionMetadata(partitionID PartitionID) {
	// Count files in partition by scanning the existing filesKV
	fileCount := 0
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)
	files, err := pm.cluster.filesKV.MapFunc(func(k, v []byte) error { return nil })
	if err != nil {
		pm.cluster.debugf("[PARTITION] Failed to count files in %s: %v", partitionID, err)
		return
	}

	for k, _ := range files {
		key := string(k)
		if strings.HasPrefix(key, prefix) && strings.HasSuffix(key, ":metadata") {
			// Check if this file has a tombstone
			parts := strings.Split(key, ":")
			if len(parts) >= 4 {
				filename := parts[3]
				tombstoneKey := fmt.Sprintf("partition:%s:file:%s:tombstone", partitionID, filename)
				if _, err := pm.cluster.filesKV.Get([]byte(tombstoneKey)); err != nil {
					// No tombstone, count this file
					fileCount++
				}
			}
		}
	}

	// Create partition info for CRDT
	partitionInfo := PartitionInfo{
		ID:           partitionID,
		Version:      PartitionVersion(time.Now().UnixNano()),
		LastModified: time.Now().Unix(),
		FileCount:    fileCount,
		Holders:      []NodeID{pm.cluster.ID}, // We hold this partition
	}

	partitionJSON, _ := json.Marshal(partitionInfo)
	partitionKey := fmt.Sprintf("partitions/%s", partitionID)

	updates := pm.cluster.frogpond.SetDataPoint(partitionKey, partitionJSON)
	if pm.cluster.crdtKV != nil {
		pm.cluster.crdtKV.Put([]byte(partitionKey), partitionJSON)
	}
	pm.cluster.sendUpdatesToPeers(updates)

	pm.cluster.debugf("[PARTITION] Updated metadata for %s: %d files", partitionID, fileCount)
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
		return fmt.Errorf("peer %s not found", peerID)
	}

	pm.cluster.Logger.Printf("[PARTITION] Starting sync of %s from %s", partitionID, peerID)

	// Request partition data from peer (use a longer timeout for large streams)
	url := fmt.Sprintf("http://%s:%d/api/partition-sync/%s", peerAddr, peerPort, partitionID)
	longClient := &http.Client{Timeout: 5 * time.Minute}
	resp, err := longClient.Get(url)
	if err != nil {
		return fmt.Errorf("failed to request partition sync: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned %s", resp.Status)
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
			return fmt.Errorf("failed to decode sync entry: %v", err)
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
				pm.cluster.Logger.Printf("[PARTITION] Failed to store sync entry %s: %v", entry.Key, err)
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
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Find next partition that needs syncing
			if partitionID, holderID := pm.findNextPartitionToSync(); partitionID != "" {
				pm.cluster.Logger.Printf("[PARTITION] Syncing %s from %s", partitionID, holderID)
				if err := pm.syncPartitionFromPeer(partitionID, holderID); err != nil {
					pm.cluster.Logger.Printf("[PARTITION] Failed to sync %s from %s: %v", partitionID, holderID, err)
					// Brief pause on error to avoid tight error loops
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

// findNextPartitionToSync finds a single partition that needs syncing
func (pm *PartitionManager) findNextPartitionToSync() (PartitionID, NodeID) {
	allPartitions := pm.getAllPartitions()
	currentRF := pm.cluster.getCurrentRF()

	// Find partitions that are under-replicated
	for partitionID, info := range allPartitions {
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

		if hasPartition {
			continue // We already have it
		}

		// Find a holder to sync from (must be a different node)
		var otherHolders []NodeID
		for _, holderID := range info.Holders {
			if holderID != pm.cluster.ID {
				otherHolders = append(otherHolders, holderID)
			}
		}

		// Only try to sync if there are other holders
		if len(otherHolders) > 0 {
			return partitionID, otherHolders[0]
		}

		// If we reach here, partition is under-replicated but we're the only holder
		// This is normal - we're the source, not the destination
	}

	return "", "" // Nothing to sync
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
