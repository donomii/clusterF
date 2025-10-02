// NEW streaming HandlePartitionSync and syncPartitionFromPeer implementations
package partitionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

// handlePartitionSync serves partition data with object-by-object streaming
func (pm *PartitionManager) HandlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID PartitionID) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx := r.Context()
	select {
	case <-ctx.Done():
		pm.debugf("[PARTITION] Client disconnected before sync of %s", partitionID)
		return
	default:
	}

	pm.debugf("[PARTITION] Starting streaming sync for partition %s", partitionID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	flusher, canFlush := w.(http.Flusher)
	prefix := fmt.Sprintf("partition:%s:file:", partitionID)

	// Stream entries one by one using FileStore
	entriesStreamed := 0
	err := pm.deps.FileStore.Scan(prefix, func(key string, metadata, content []byte) error {
		select {
		case <-ctx.Done():
			return fmt.Errorf("client disconnected")
		default:
		}

		if !strings.HasPrefix(key, prefix) {
			return nil
		}

		// Get content for this key
		if content == nil {
			content = []byte{}
		}

		// Calculate checksum
		checksum := pm.calculateEntryChecksum(metadata, content)

		entry := PartitionSyncEntry{
			Key:      key,
			Metadata: metadata,
			Content:  content,
			Checksum: checksum,
		}

		// Stream this entry
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode entry %s: %v", key, err)
		}

		entriesStreamed++
		if entriesStreamed%10 == 0 && canFlush {
			flusher.Flush()
			pm.debugf("[PARTITION] Streamed %d entries for %s", entriesStreamed, partitionID)
		}

		return nil
	})

	if err != nil {
		pm.logf("[PARTITION] Failed to stream partition %s: %v", partitionID, err)
		return
	}

	if canFlush {
		flusher.Flush()
	}

	pm.debugf("[PARTITION] Completed streaming %d entries for partition %s", entriesStreamed, partitionID)
}

// syncPartitionFromPeer synchronizes a partition using object-by-object streaming
func (pm *PartitionManager) syncPartitionFromPeer(ctx context.Context, partitionID PartitionID, peerID types.NodeID) error {
	// No-store nodes should never sync partitions
	if pm.deps.NoStore {
		pm.debugf("[PARTITION] No-store mode: refusing to sync partition %s", partitionID)
		return fmt.Errorf("no-store mode: cannot sync partitions")
	}

	if !pm.hasFrogpond() {
		return fmt.Errorf("frogpond node is not configured")
	}

	// Find peer address
	var peerAddr string
	var peerPort int
	peers := pm.getPeers()
	for _, peer := range peers {
		if types.NodeID(peer.NodeID) == peerID {
			peerAddr = peer.Address
			peerPort = peer.HTTPPort
			break
		}
	}

	if peerAddr == "" {
		availablePeers := make([]string, 0, len(peers))
		for _, peer := range peers {
			availablePeers = append(availablePeers, fmt.Sprintf("%s@%s:%d", peer.NodeID, peer.Address, peer.HTTPPort))
		}
		return fmt.Errorf("peer '%s' not found for partition '%s'. Available peers: [%s]", peerID, partitionID, strings.Join(availablePeers, ", "))
	}

	pm.logf("[PARTITION] Starting streaming sync of %s from %s", partitionID, peerID)

	// Request partition data stream from peer
	syncURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return fmt.Errorf("failed to build sync URL: %v", err)
	}

	longClient := &http.Client{Timeout: 10 * time.Minute}
	resp, err := longClient.Get(syncURL)
	if err != nil {
		return fmt.Errorf("failed to request partition sync: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned error %d '%s'", resp.StatusCode, resp.Status)
	}

	// Stream and apply entries one by one
	decoder := json.NewDecoder(resp.Body)
	syncCount := 0

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context canceled: %v", ctx.Err())
		default:
		}

		var entry PartitionSyncEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to decode sync entry: %v", err)
		}

		// Verify entry checksum
		expectedChecksum := pm.calculateEntryChecksum(entry.Metadata, entry.Content)
		if entry.Checksum != expectedChecksum {
			pm.logf("[PARTITION] Checksum mismatch for %s from %s: expected %s, got %s", entry.Key, peerID, expectedChecksum, entry.Checksum)
			continue // Skip corrupted entry
		}

		// Apply CRDT merge logic
		if pm.shouldUpdateEntry(entry) {
			if err := pm.deps.FileStore.Put(entry.Key, entry.Metadata, entry.Content); err != nil {
				pm.logf("[PARTITION] Failed to store sync entry '%s': %v", entry.Key, err)
				continue
			}
			syncCount++

			if syncCount%100 == 0 {
				pm.debugf("[PARTITION] Applied %d entries for partition %s", syncCount, partitionID)
			}
		}
	}

	// Update our partition metadata
	pm.updatePartitionMetadata(partitionID)

	pm.logf("[PARTITION] Completed sync of %s from %s (%d entries applied)", partitionID, peerID, syncCount)

	// Notify frontend that file list may have changed
	pm.notifyFileListChanged()

	return nil
}

// shouldUpdateEntry determines if we should update our local copy with the remote entry
func (pm *PartitionManager) shouldUpdateEntry(remoteEntry PartitionSyncEntry) bool {
	// Get our current version
	localData, err := pm.deps.FileStore.Get(remoteEntry.Key)
	if err != nil || !localData.Exists {
		// We don't have this entry, accept it
		return true
	}

	// Parse both metadata for CRDT comparison
	var localMetadata, remoteMetadata map[string]interface{}
	if err := json.Unmarshal(localData.Metadata, &localMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse local metadata for %s: %v", remoteEntry.Key, err)
		return true // Accept remote if we can't parse local
	}
	if err := json.Unmarshal(remoteEntry.Metadata, &remoteMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse remote metadata for %s: %v", remoteEntry.Key, err)
		return false // Reject remote if we can't parse it
	}

	// CRDT rule: use latest timestamp, break ties with version
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

	// Remote is newer
	if remoteTime > localTime {
		return true
	}

	// Same timestamp, use version to break tie
	if remoteTime == localTime {
		remoteVersion, _ := remoteMetadata["version"].(float64)
		localVersion, _ := localMetadata["version"].(float64)
		return remoteVersion > localVersion
	}

	// Local is newer or same
	return false
}
