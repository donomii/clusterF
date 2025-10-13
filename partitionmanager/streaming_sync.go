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
	"github.com/donomii/frogpond"
)

// handlePartitionSync serves partition data with object-by-object streaming
func (pm *PartitionManager) HandlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
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

	if pm.deps.Cluster.GetPartitionSyncPaused() {
		http.Error(w, "sync paused", http.StatusServiceUnavailable)
		return
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

	// Stream an empty object to signify end of stream
	// This is needed because JSON decoder expects a valid JSON value
	endMarker := struct{}{}
	if err := encoder.Encode(endMarker); err != nil {
		pm.logf("[PARTITION] Failed to send end marker for partition %s: %v", partitionID, err)
		return
	}

	if canFlush {
		flusher.Flush()
	}

	pm.MarkForReindex(partitionID)

	pm.debugf("[PARTITION] Completed streaming %d entries for partition %s", entriesStreamed, partitionID)
}

// syncPartitionFromPeer synchronizes a partition using object-by-object streaming
func (pm *PartitionManager) syncPartitionFromPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID) error {
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
		if ctx.Err() != nil {
			return fmt.Errorf("context canceled: %v", ctx.Err())
		}
		if types.NodeID(peer.NodeID) == peerID {
			peerAddr = peer.Address
			peerPort = peer.HTTPPort
			break
		}
	}

	if peerAddr == "" {
		// Remove the requested peer as a holder for this partition.  Backdate the entry by 1 hr
		pm.removePeerHolder(partitionID, peerID, time.Now().Add(-1*time.Hour))

		availablePeers := make([]string, 0, len(peers))
		for _, peer := range peers {
			availablePeers = append(availablePeers, fmt.Sprintf("%s@%s:%d", peer.NodeID, peer.Address, peer.HTTPPort))
		}
		return fmt.Errorf("peer '%s' not found for partition '%s'. Available peers: [%s]", peerID, partitionID, strings.Join(availablePeers, ", "))
	}

	pm.debugf("[PARTITION] Starting streaming sync of %s from %s", partitionID, peerID)

	// Request partition data stream from peer
	syncURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return fmt.Errorf("failed to build sync URL: %v", err)
	}

	resp, err := pm.httpClient().Get(syncURL)
	if err != nil {
		return fmt.Errorf("failed to request partition sync: %v", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer returned error %d '%s'", resp.StatusCode, resp.Status)
	}

	// Stream and apply entries one by one
	decoder := json.NewDecoder(resp.Body)
	syncCount := 0

	for {
		if ctx.Err() != nil {
			return fmt.Errorf("context canceled: %v", ctx.Err())
		}

		var entry PartitionSyncEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			// Read a sample of what we received to help debug
			sample := make([]byte, 200)
			n, _ := resp.Body.Read(sample)
			return fmt.Errorf("failed to decode sync entry from %s (partition %s): %v. Sample of received data (%d bytes): %q", peerID, partitionID, err, n, string(sample[:n]))
		}

		if entry.Key == "" {
			break // End of stream marker
		}

		// Verify entry checksum
		expectedChecksum := pm.calculateEntryChecksum(entry.Metadata, entry.Content)
		if entry.Checksum != expectedChecksum {
			pm.logf("[PARTITION] Checksum mismatch for %s from %s: expected %s, got %s", entry.Key, peerID, expectedChecksum, entry.Checksum)
			//FIXME do something about it
			continue // Skip corrupted entry
		}

		// Apply CRDT merge logic
		if pm.shouldUpdateEntry(entry) {
			if err := pm.deps.FileStore.Put(entry.Key, entry.Metadata, entry.Content); err != nil {
				pm.logf("[PARTITION] Failed to store sync entry '%s': %v", entry.Key, err)
				continue
			}
			syncCount++

			var metadata types.FileMetadata
			err := json.Unmarshal(entry.Metadata, &metadata)
			if err != nil {
				pm.logf("corrupt metadata in transfer: %v", string(entry.Metadata))
			}

			partitionKey := HashToPartition(metadata.Path)
			holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, pm.deps.NodeID)

			// Get existing partitiontime
			dataPoint := pm.deps.Frogpond.GetDataPoint(holderKey)
			data := dataPoint.Value

			var currentHolderData types.HolderData
			json.Unmarshal(data, &currentHolderData)

			if metadata.ModifiedAt.After(currentHolderData.Last_update) {
				holderData := types.HolderData{
					Last_update: metadata.ModifiedAt,
					File_count:  currentHolderData.File_count, //FIXME detect if we're updating or inserting for the first time
					Checksum:    "",
				}
				holderJSON, _ := json.Marshal(holderData)

				// Send  updates to CRDT
				pm.sendUpdates(pm.deps.Frogpond.SetDataPoint(holderKey, holderJSON))
			}

			if syncCount%100 == 0 {
				pm.debugf("[PARTITION] Applied %d entries for partition %s", syncCount, partitionID)
			}
		}
	}

	// Update our partition metadata
	pm.MarkForReindex(partitionID)

	pm.debugf("[PARTITION] Completed sync of %s from %s (%d entries applied)", partitionID, peerID, syncCount)

	// Notify frontend that file list may have changed
	pm.notifyFileListChanged()

	return nil
}

func (pm *PartitionManager) removePeerHolder(partitionID types.PartitionID, peerID types.NodeID, backdate time.Time) {
	if !pm.hasFrogpond() {
		return
	}

	if pm.deps.NoStore {
		return // No-store nodes don't claim to hold partitions
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionID)
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, peerID)

	// Create a backdated tombstone
	tombstone := frogpond.DataPoint{
		Key:     []byte(holderKey),
		Value:   nil,
		Name:    holderKey,
		Updated: backdate,
		Deleted: true,
	}

	pm.sendUpdates([]frogpond.DataPoint{tombstone})

	pm.debugf("[PARTITION] Removed %s as holder for %s", peerID, partitionID)
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
	var localMetadata, remoteMetadata types.FileMetadata
	if err := json.Unmarshal(localData.Metadata, &localMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse local metadata for %s: %v", remoteEntry.Key, err)
		return true // Accept remote if we can't parse local
	}
	if err := json.Unmarshal(remoteEntry.Metadata, &remoteMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse remote metadata for %s: %v", remoteEntry.Key, err)
		return false // Reject remote if we can't parse it
	}

	// CRDT rule: use latest timestamp, break ties with version
	remoteTime := remoteMetadata.ModifiedAt
	localTime := localMetadata.ModifiedAt
	remoteDeletedAt := remoteMetadata.DeletedAt
	localDeletedAt := localMetadata.DeletedAt

	// Use the latest of modified_at or deleted_at
	if remoteDeletedAt.After(remoteTime) {
		remoteTime = remoteDeletedAt
	}
	if localDeletedAt.After(localTime) {
		localTime = localDeletedAt
	}

	// Remote is newer
	if remoteTime.After(localTime) {
		return true
	}

	// Same timestamp, use alphabetical path name
	if remoteTime.Equal(localTime) {
		return localMetadata.Path < remoteMetadata.Path
	}

	// Local is newer or same
	return false
}
