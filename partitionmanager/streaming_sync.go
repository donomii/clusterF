// NEW streaming HandlePartitionSync and syncPartitionWithPeer implementations
package partitionmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

// handlePartitionSync serves partition data with object-by-object streaming or accepts updates
func (pm *PartitionManager) HandlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
	switch r.Method {
	case http.MethodGet:
		pm.handlePartitionSyncGet(w, r, partitionID)
	case http.MethodPost:
		pm.handlePartitionSyncPost(w, r, partitionID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (pm *PartitionManager) handlePartitionSyncGet(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
	ctx := r.Context()
	select {
	case <-ctx.Done():
		pm.debugf("[PARTITION] Client disconnected before sync of %s", partitionID)
		return
	default:
	}

	if pm.deps.Cluster != nil && pm.deps.Cluster.GetPartitionSyncPaused() {
		http.Error(w, "sync paused", http.StatusServiceUnavailable)
		return
	}
	pm.debugf("[PARTITION] Starting streaming sync for partition %s", partitionID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	encoder := json.NewEncoder(w)
	flusher, canFlush := w.(http.Flusher)

	paths, err := pm.partitionPaths(partitionID)
	if err != nil {
		pm.logf("[PARTITION] Failed to list files for partition %s: %v", partitionID, err)
		http.Error(w, "failed to enumerate partition", http.StatusInternalServerError)
		return
	}

	entriesStreamed := 0
	for _, path := range paths {
		select {
		case <-ctx.Done():
			pm.debugf("[PARTITION] Client disconnected while streaming %s", partitionID)
			return
		default:
		}

		entry, entryErr := pm.buildPartitionEntry(partitionID, path)
		if entryErr != nil {
			pm.debugf("[PARTITION] Skipping %s in %s: %v", path, partitionID, entryErr)
			continue
		}

		if err := encoder.Encode(entry); err != nil {
			pm.logf("[PARTITION] Failed to encode entry %s in %s: %v", path, partitionID, err)
			return
		}

		entriesStreamed++
		if entriesStreamed%10 == 0 && canFlush {
			flusher.Flush()
			pm.debugf("[PARTITION] Streamed %d entries for %s", entriesStreamed, partitionID)
		}
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

func (pm *PartitionManager) handlePartitionSyncPost(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
	if pm.deps.Cluster != nil && pm.deps.Cluster.GetPartitionSyncPaused() {
		http.Error(w, "sync paused", http.StatusServiceUnavailable)
		return
	}

	sourceNode := types.NodeID(r.Header.Get("X-ClusterF-Node"))
	if sourceNode == "" {
		sourceNode = "unknown"
	}

	decoder := json.NewDecoder(r.Body)
	applied := 0
	skipped := 0

	for {
		var entry PartitionSyncEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			http.Error(w, fmt.Sprintf("failed to decode entry: %v", err), http.StatusBadRequest)
			return
		}

		if entry.Path == "" {
			break
		}

		ok, err := pm.applyPartitionEntry(entry, sourceNode)
		if err != nil {
			pm.logf("[PARTITION] Failed to apply streamed entry %s (partition %s) from %s: %v", entry.Path, types.PartitionIDForPath(entry.Path), sourceNode, err)
			continue
		}
		if ok {
			applied++
		} else {
			skipped++
		}
	}

	pm.MarkForReindex(partitionID)
	pm.notifyFileListChanged()

	response := map[string]int{
		"applied": applied,
		"skipped": skipped,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	pm.debugf("[PARTITION] Applied %d entries from %s for partition %s (skipped %d)", applied, sourceNode, partitionID, skipped)
}

// syncPartitionWithPeer synchronizes a partition using object-by-object streaming in both directions
func (pm *PartitionManager) syncPartitionWithPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID) error {
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
		pm.removePeerHolder(partitionID, peerID, 1*time.Hour)

		availablePeers := make([]string, 0, len(peers))
		for _, peer := range peers {
			availablePeers = append(availablePeers, fmt.Sprintf("%s@%s:%d", peer.NodeID, peer.Address, peer.HTTPPort))
		}
		return fmt.Errorf("peer '%s' not found for partition '%s'. Available peers: [%s]", peerID, partitionID, strings.Join(availablePeers, ", "))
	}

	pm.debugf("[PARTITION] Starting bidirectional streaming sync of %s with %s", partitionID, peerID)

	syncCount, err := pm.downloadPartitionFromPeer(ctx, partitionID, peerID, peerAddr, peerPort)
	if err != nil {
		return err
	}

	pm.MarkForReindex(partitionID)

	pm.debugf("[PARTITION] Completed inbound sync of %s from %s (%d entries applied)", partitionID, peerID, syncCount)

	// Notify frontend that file list may have changed
	pm.notifyFileListChanged()

	// Push our latest view back to the peer
	if err := pm.pushPartitionToPeer(ctx, partitionID, peerID, peerAddr, peerPort); err != nil {
		return fmt.Errorf("failed to push partition %s to %s: %w", partitionID, peerID, err)
	}

	pm.debugf("[PARTITION] Completed bidirectional sync of %s with %s", partitionID, peerID)

	return nil
}

func (pm *PartitionManager) downloadPartitionFromPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID, peerAddr string, peerPort int) (int, error) {
	syncURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return 0, fmt.Errorf("failed to build sync URL: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, syncURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to build sync request: %v", err)
	}

	resp, err := pm.httpClient().Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to request partition sync: %v", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("peer returned error %d '%s'", resp.StatusCode, resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	syncCount := 0

	for {
		if ctx.Err() != nil {
			return 0, fmt.Errorf("context canceled: %v", ctx.Err())
		}

		var entry PartitionSyncEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			sample := make([]byte, 200)
			n, _ := resp.Body.Read(sample)
			return 0, fmt.Errorf("failed to decode sync entry from %s (partition %s): %v. Sample of received data (%d bytes): %q", peerID, partitionID, err, n, string(sample[:n]))
		}

		if entry.Path == "" {
			break
		}

		applied, err := pm.applyPartitionEntry(entry, peerID)
		if err != nil {
			pm.logf("[PARTITION] Failed to apply sync entry '%s' (partition %s) from %s: %v", entry.Path, types.PartitionIDForPath(entry.Path), peerID, err)
			continue
		}
		if applied {
			syncCount++
			if syncCount%100 == 0 {
				pm.debugf("[PARTITION] Applied %d entries for partition %s", syncCount, partitionID)
			}
		}
	}

	return syncCount, nil
}

func (pm *PartitionManager) applyPartitionEntry(entry PartitionSyncEntry, peerID types.NodeID) (bool, error) {
	expectedChecksum := pm.calculateEntryChecksum(entry.Metadata, entry.Content)
	if entry.Checksum != expectedChecksum {
		return false, fmt.Errorf("checksum mismatch for %s from %s: expected %s, got %s", entry.Path, peerID, expectedChecksum, entry.Checksum)
	}

	if !pm.shouldUpdateEntry(entry) {
		return false, nil
	}

	pm.recordEssentialDiskActivity()
	if err := pm.deps.FileStore.Put(entry.Path, entry.Metadata, entry.Content); err != nil {
		return false, fmt.Errorf("failed to store entry %s: %w", entry.Path, err)
	}

	var metadata types.FileMetadata
	if err := json.Unmarshal(entry.Metadata, &metadata); err != nil {
		return true, pm.errorf(entry.Metadata, fmt.Sprintf("corrupt metadata for entry %s: %v", entry.Path, err))
	}
	if metadata.Path == "" {
		metadata.Path = entry.Path
	}

	if pm.deps.Indexer != nil {
		pm.deps.Indexer.AddFile(metadata.Path, metadata)
	}

	if !pm.hasFrogpond() {
		return true, nil
	}

	partitionKey := HashToPartition(metadata.Path)
	holderKey := fmt.Sprintf("partitions/%s/holders/%s", partitionKey, pm.deps.NodeID)

	dataPoint := pm.deps.Frogpond.GetDataPoint(holderKey)
	var currentHolderData types.HolderData
	if len(dataPoint.Value) > 0 {
		if err := json.Unmarshal(dataPoint.Value, &currentHolderData); err != nil {
			currentHolderData = types.HolderData{}
		}
	}

	if metadata.LastClusterUpdate.After(currentHolderData.MostRecentModifiedTime) {
		holderData := types.HolderData{
			MostRecentModifiedTime: metadata.LastClusterUpdate,
			File_count:             currentHolderData.File_count,
			Checksum:               "",
		}
		holderJSON, _ := json.Marshal(holderData)
		pm.sendUpdates(pm.deps.Frogpond.SetDataPoint(holderKey, holderJSON))
	}

	return true, nil
}

func (pm *PartitionManager) partitionPaths(partitionID types.PartitionID) ([]string, error) {
	if pm.deps.Indexer != nil {
		return pm.deps.Indexer.FilesForPartition(partitionID), nil
	}

	pm.recordEssentialDiskActivity()
	pm.debugf("[PARTITION] Indexer unavailable, falling back to metadata scan for %s", partitionID)
	paths := []string{}
	err := pm.deps.FileStore.ScanMetadata("", func(path string, _ []byte) error {
		if types.PartitionIDForPath(path) != partitionID {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func (pm *PartitionManager) buildPartitionEntry(partitionID types.PartitionID, path string) (PartitionSyncEntry, error) {
	pm.recordEssentialDiskActivity()
	data, err := pm.deps.FileStore.Get(path)
	if err != nil {
		return PartitionSyncEntry{}, err
	}
	if data == nil || !data.Exists {
		return PartitionSyncEntry{}, fmt.Errorf("entry %s missing", path)
	}
	metadata := data.Metadata
	if len(metadata) == 0 {
		return PartitionSyncEntry{}, fmt.Errorf("entry %s has no metadata", path)
	}
	content := data.Content
	if content == nil {
		content = []byte{}
	}
	return PartitionSyncEntry{
		Path:     path,
		Metadata: metadata,
		Content:  content,
		Checksum: pm.calculateEntryChecksum(metadata, content),
	}, nil
}

func (pm *PartitionManager) pushPartitionToPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID, peerAddr string, peerPort int) error {
	pushURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return fmt.Errorf("failed to build push URL: %v", err)
	}

	pr, pw := io.Pipe()
	encoder := json.NewEncoder(pw)
	errCh := make(chan error, 1)
	var sentCount int64

	go func() {
		paths, listErr := pm.partitionPaths(partitionID)
		if listErr != nil {
			pw.CloseWithError(listErr)
			errCh <- listErr
			return
		}

		for _, path := range paths {
			if ctx.Err() != nil {
				err := ctx.Err()
				pw.CloseWithError(err)
				errCh <- err
				return
			}

			entry, entryErr := pm.buildPartitionEntry(partitionID, path)
			if entryErr != nil {
				pm.debugf("[PARTITION] Skipping %s in %s while pushing to %s: %v", path, partitionID, peerID, entryErr)
				continue
			}

			if err := encoder.Encode(entry); err != nil {
				pw.CloseWithError(err)
				errCh <- err
				return
			}

			sentCount++
			if sentCount%100 == 0 {
				pm.debugf("[PARTITION] Streamed %d entries of %s to %s", sentCount, partitionID, peerID)
			}
		}

		if err := encoder.Encode(struct{}{}); err != nil {
			pw.CloseWithError(err)
			errCh <- err
			return
		}

		pw.Close()
		errCh <- nil
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pushURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		<-errCh
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClusterF-Node", string(pm.deps.NodeID))

	resp, err := pm.httpClient().Do(req)
	if err != nil {
		pw.CloseWithError(err)
		<-errCh
		return err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		streamErr := <-errCh
		msg := strings.TrimSpace(string(body))
		if msg == "" {
			msg = resp.Status
		}
		if streamErr != nil {
			return fmt.Errorf("peer %s rejected partition push (%s): %v", peerID, msg, streamErr)
		}
		return fmt.Errorf("peer %s rejected partition push (%s)", peerID, msg)
	}

	streamErr := <-errCh
	if streamErr != nil && streamErr != io.EOF {
		return fmt.Errorf("failed to stream partition %s to %s: %w", partitionID, peerID, streamErr)
	}

	pm.debugf("[PARTITION] Pushed %d entries for partition %s to %s", sentCount, partitionID, peerID)
	return nil
}

func (pm *PartitionManager) removePeerHolder(partitionID types.PartitionID, peerID types.NodeID, backdate time.Duration) {
	if !pm.hasFrogpond() {
		return
	}

	if pm.deps.NoStore {
		return // No-store nodes don't claim to hold partitions
	}

	partitionKey := fmt.Sprintf("partitions/%s", partitionID)
	holderKey := fmt.Sprintf("%s/holders/%s", partitionKey, peerID)

	dps := pm.deps.Frogpond.DeleteDataPoint(holderKey, backdate)

	pm.sendUpdates(dps)

	pm.debugf("[PARTITION] Removed %s as holder for %s", peerID, partitionID)
}

// shouldUpdateEntry determines if we should update our local copy with the remote entry
func (pm *PartitionManager) shouldUpdateEntry(remoteEntry PartitionSyncEntry) bool {
	pm.recordEssentialDiskActivity()
	// Get our current version
	localData, err := pm.deps.FileStore.Get(remoteEntry.Path)
	if err != nil || !localData.Exists {
		// We don't have this entry, accept it
		return true
	}

	// Parse both metadata for CRDT comparison
	var localMetadata, remoteMetadata types.FileMetadata
	if err := json.Unmarshal(localData.Metadata, &localMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse local metadata for %s: %v", remoteEntry.Path, err)
		return true // Accept remote if we can't parse local
	}
	if err := json.Unmarshal(remoteEntry.Metadata, &remoteMetadata); err != nil {
		pm.debugf("[PARTITION] Failed to parse remote metadata for %s: %v", remoteEntry.Path, err)
		return false // Reject remote if we can't parse it
	}

	// CRDT rule: use latest LastClusterUpdate timestamp, break ties with version
	remoteTime := remoteMetadata.LastClusterUpdate
	localTime := localMetadata.LastClusterUpdate

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
