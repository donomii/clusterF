// partitions.go - Partitioning system for scalable file storage using existing KV stores
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
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
func (pm *PartitionManager) hashToPartition(filename string) PartitionID {
	h := crc32.ChecksumIEEE([]byte(filename))
	partitionNum := h % DefaultPartitionCount
	return PartitionID(fmt.Sprintf("p%05d", partitionNum))
}

// storeFileInPartition stores a file in its appropriate partition using existing KV store
func (pm *PartitionManager) storeFileInPartition(filename string, content []byte, contentType string) error {
	partitionID := pm.hashToPartition(filename)

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

// getFileFromPartition retrieves a file from its partition using existing KV store
func (pm *PartitionManager) getFileFromPartition(filename string) ([]byte, map[string]interface{}, error) {
	partitionID := pm.hashToPartition(filename)

	// Get from existing filesKV using partition-prefixed keys
	contentKey := fmt.Sprintf("partition:%s:file:%s:content", partitionID, filename)
	metadataKey := fmt.Sprintf("partition:%s:file:%s:metadata", partitionID, filename)

	content, err := pm.cluster.filesKV.Get([]byte(contentKey))
	if err != nil {
		return nil, nil, fmt.Errorf("file not found: %v", err)
	}

	metadataBytes, err := pm.cluster.filesKV.Get([]byte(metadataKey))
	if err != nil {
		return content, nil, nil // Return content even if metadata is missing
	}

	var metadata map[string]interface{}
	json.Unmarshal(metadataBytes, &metadata)

	return content, metadata, nil
}

// deleteFileFromPartition removes a file from its partition using existing KV store
func (pm *PartitionManager) deleteFileFromPartition(filename string) error {
	partitionID := pm.hashToPartition(filename)

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

// listFilesInPartition lists all files in a specific partition using existing KV store
func (pm *PartitionManager) listFilesInPartition(partitionID PartitionID) ([]string, error) {
	var files []string
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)

	pm.cluster.filesKV.MapFunc(func(k, v []byte) error {
		key := string(k)
		if strings.HasPrefix(key, prefix) && strings.HasSuffix(key, ":metadata") {
			// Extract filename from "partition:pXXXXX:file:{filename}:metadata"
			parts := strings.Split(key, ":")
			if len(parts) >= 4 {
				filename := parts[3]

				// Check if file has tombstone
				tombstoneKey := fmt.Sprintf("partition:%s:file:%s:tombstone", partitionID, filename)
				if _, err := pm.cluster.filesKV.Get([]byte(tombstoneKey)); err != nil {
					// No tombstone, file exists
					files = append(files, filename)
				}
			}
		}
		return nil
	})

	sort.Strings(files)
	return files, nil
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

		// Find a holder to sync from
		if len(info.Holders) > 0 {
			for _, holderID := range info.Holders {
				if holderID != pm.cluster.ID {
					return partitionID, holderID
				}
			}
		}
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
