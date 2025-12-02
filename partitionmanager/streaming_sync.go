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

	"github.com/donomii/clusterF/httpclient"
	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
)

// handlePartitionSync serves partition data with object-by-object streaming or accepts updates
func (pm *PartitionManager) HandlePartitionSync(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
	if pm.deps.Cluster.NoStore() {
		http.Error(w, "no store", http.StatusServiceUnavailable)
		return
	}
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
	if pm.deps.Cluster.NoStore() {
		http.Error(w, "no store", http.StatusServiceUnavailable)
		return
	}

	if pm.deps.Cluster != nil && pm.deps.Cluster.GetPartitionSyncPaused() {
		http.Error(w, "sync paused", http.StatusServiceUnavailable)
		return
	}
	pm.debugf("[PARTITION GET] Starting streaming sync for partition %s", partitionID)

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

	pm.debugf("[PARTITION SYNC] Streaming %d files for partition %s", len(paths), partitionID)
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

		encodeErr := encoder.Encode(entry)
		if encodeErr != nil {
			pm.logf("[PARTITION] Failed to encode entry %s in %s: %v", path, partitionID, encodeErr)
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
	endErr := encoder.Encode(endMarker)
	if endErr != nil {
		pm.logf("[PARTITION] Failed to send end marker for partition %s: %v", partitionID, endErr)
		return
	}

	if canFlush {
		flusher.Flush()
	}

	pm.debugf("[PARTITION] Completed streaming %d entries for partition %s", entriesStreamed, partitionID)
}

func (pm *PartitionManager) handlePartitionSyncPost(w http.ResponseWriter, r *http.Request, partitionID types.PartitionID) {
	syncStart := time.Now()
	if pm.deps.Cluster != nil && pm.deps.Cluster.GetPartitionSyncPaused() {
		http.Error(w, "sync paused", http.StatusServiceUnavailable)
		return
	}

	if pm.deps.Cluster.NoStore() {
		http.Error(w, "no store", http.StatusServiceUnavailable)
		return
	}

	sourceNode := types.NodeID(r.Header.Get("X-ClusterF-Node"))
	if sourceNode == "" {
		sourceNode = "unknown"
	}

	sourcePeer, err := pm.resolvePeerForSync(sourceNode)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to resolve peer info for %s: %v", sourceNode, err), http.StatusBadRequest)
		return
	}

	decoder := json.NewDecoder(r.Body)
	ctx := r.Context()
	applied, skipped, fetchErr, decodeErr := pm.processPartitionEntryStream(ctx, decoder, sourcePeer)
	if decodeErr != nil {
		http.Error(w, fmt.Sprintf("failed to decode entry: %v", decodeErr), http.StatusBadRequest)
		return
	}
	if fetchErr != nil {
		pm.logf("[PARTITION] Errors fetching entries from %s: %v", sourceNode, fetchErr)
	}

	pm.notifyFileListChanged()

	pm.recordPartitionTimestamp(partitionID, lastSyncTimestampFile, syncStart) //TODO I'm not sure if this is safe

	response := map[string]int{
		"applied": applied,
		"skipped": skipped,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
	pm.debugf("[PARTITION] Applied %d entries from %s for partition %s (skipped %d)", applied, sourceNode, partitionID, skipped)
	if applied > 0 {
		pm.logf("[PARTITION] Applied %d entries from %s for partition %s (skipped %d)", applied, sourceNode, partitionID, skipped)
	}
}

// syncPartitionWithPeer synchronizes a partition using object-by-object streaming in both directions
func (pm *PartitionManager) syncPartitionWithPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID) error {
	syncStart := time.Now()
	// No-store nodes should never sync partitions
	if pm.deps.Cluster.NoStore() {
		pm.debugf("[PARTITION SYNCPEER] No-store mode: refusing to sync partition %s", partitionID)
		return fmt.Errorf("no-store mode: cannot sync partitions")
	}

	if !pm.hasFrogpond() {
		return fmt.Errorf("frogpond node is not configured")
	}

	peer := pm.getPeer(peerID)
	if peer == nil {
		return fmt.Errorf("peer %s not found", peerID)
	}

	if peer.Address == "" {
		// Remove the requested peer as a holder for this partition.  Backdate the entry by 1 hr
		pm.removePeerHolder(partitionID, peerID, 1*time.Hour)
		pm.logf("[PARTITION SYNCPEER] Removed %s as holder for %s because not peer not found", peerID, partitionID)

		availablePeers := make([]string, 0)
		for _, peer := range pm.getPeers() {
			availablePeers = append(availablePeers, fmt.Sprintf("%s@%s:%d", peer.NodeID, peer.Address, peer.HTTPPort))
		}
		return fmt.Errorf("peer '%s' not found for partition '%s'. Available peers: [%s]", peerID, partitionID, strings.Join(availablePeers, ", "))
	}

	pm.debugf("[PARTITION SYNCPEER] Starting bidirectional streaming sync of %s with %s, %v:%v", partitionID, peerID, peer.Address, peer.HTTPPort)

	applied, _, err := pm.downloadPartitionFromPeer(ctx, partitionID, peerID, peer.Address, peer.HTTPPort)
	if err != nil {
		return err
	}

	if applied > 0 {
		pm.MarkForReindex(partitionID, fmt.Sprintf("applied %d entries from peer %s", applied, peerID))
	}

	pm.debugf("[PARTITION SYNCPEER] Completed inbound sync of %s from %s (%d entries applied)", partitionID, peerID, applied)
	if applied > 0 {
		pm.logf("[PARTITION SYNCPEER] Completed inbound sync of %s from %s (%d entries applied)", partitionID, peerID, applied)
	}

	// Notify frontend that file list may have changed
	pm.notifyFileListChanged()

	// Push our latest view back to the peer
	_, pushErr := pm.pushPartitionToPeer(ctx, partitionID, peerID, peer.Address, peer.HTTPPort)
	if pushErr != nil {
		return fmt.Errorf("failed to push partition %s to %s: %w", partitionID, peerID, pushErr)
	}

	pm.recordPartitionTimestamp(partitionID, lastSyncTimestampFile, syncStart)

	pm.debugf("[PARTITION SYNCPEER] Completed bidirectional sync of %s with %s", partitionID, peerID)

	return nil
}

func (pm *PartitionManager) downloadPartitionFromPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID, peerAddr string, peerPort int) (int, int, error) {
	if pm.deps.Cluster.NoStore() {
		return 0, 0, fmt.Errorf("no store")
	}
	syncURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to build sync URL: %v", err)
	}
	pm.debugf("[PARTITION DOWNLOAD] Downloading partition %s from %s", partitionID, syncURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, syncURL, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to build sync request: %v", err)
	}

	resp, err := pm.httpClient().Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to request partition sync: %v", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		pm.RemoveNodeFromPartitionWithTimestamp(peerID, string(partitionID), time.Now().Add(-30*time.Minute))
		pm.logf("[PARTITION DOWNLOAD] Removed %s as holder for %s because sync failed with %d", peerID, partitionID, resp.StatusCode)
		return 0, 0, fmt.Errorf("peer returned error %d '%s'", resp.StatusCode, resp.Status)
	}

	decoder := json.NewDecoder(resp.Body)
	peer := &types.PeerInfo{
		NodeID:   peerID,
		Address:  peerAddr,
		HTTPPort: peerPort,
	}
	applied, skipped, fetchErr, decodeErr := pm.processPartitionEntryStream(ctx, decoder, peer)
	if decodeErr != nil {
		return applied, skipped, decodeErr
	}
	if fetchErr != nil {
		return applied, skipped, fetchErr
	}

	return applied, skipped, nil
}

func (pm *PartitionManager) fetchFileContentFromPeer(ctx context.Context, peer *types.PeerInfo, path string) ([]byte, error) {
	if pm.deps.Cluster.NoStore() {
		return nil, fmt.Errorf("no store")
	}
	fileURL, err := urlutil.BuildInternalFilesURL(peer.Address, peer.HTTPPort, path)
	if err != nil {
		return nil, err
	}

	content, _, status, err := httpclient.SimpleGet(ctx, pm.httpClient(), fileURL,
		httpclient.WithHeader("X-ClusterF-Internal", "1"),
	)
	if err != nil {
		return nil, err
	}

	if status != http.StatusOK {
		msg := strings.TrimSpace(string(content))
		if msg == "" {
			msg = fmt.Sprintf("status %d", status)
		} else {
			msg = fmt.Sprintf("%d %s", status, msg)
		}
		return nil, fmt.Errorf("failed to fetch %s from %s via internal API: %s", path, peer.NodeID, msg)
	}

	return content, nil
}

func (pm *PartitionManager) storeEntryMetadata(entry PartitionSyncEntry) error {
	if pm.deps.Cluster.NoStore() {
		return fmt.Errorf("no store")
	}
	if entry.Metadata.Path == "" {
		panic("what the fuck were you thinking you stupid AI")
	}
	pm.recordEssentialDiskActivity()
	metadataBytes, marshalErr := json.Marshal(entry.Metadata)
	if marshalErr != nil {
		return fmt.Errorf("failed to encode metadata for entry %s: %v", entry.Path, marshalErr)
	}

	// Deleted entries wipe content; directories keep existing content untouched.
	if entry.Metadata.Deleted {
		putErr := pm.deps.FileStore.Put(entry.Path, metadataBytes, []byte{})
		if putErr != nil {
			return fmt.Errorf("failed to store tombstone for %s: %w", entry.Path, putErr)
		}
	} else {
		panic("Cannot store metadata without content")
	}

	if pm.deps.Indexer != nil {
		pm.deps.Indexer.AddFile(entry.Metadata.Path, entry.Metadata)
	}

	return nil
}

func (pm *PartitionManager) storeEntryMetadataAndContent(entry PartitionSyncEntry, content []byte) error {
	if entry.Metadata.Path == "" {
		panic("what the fuck were you thinking you stupid AI")
	}
	if pm.deps.Cluster.NoStore() {
		return fmt.Errorf("no store")
	}
	pm.recordEssentialDiskActivity()
	metadataBytes, marshalErr := json.Marshal(entry.Metadata)
	if marshalErr != nil {
		return fmt.Errorf("failed to encode metadata for entry %s: %v", entry.Path, marshalErr)
	}

	putErr := pm.deps.FileStore.Put(entry.Path, metadataBytes, content)
	if putErr != nil {
		return fmt.Errorf("failed to store entry %s: %w", entry.Path, putErr)
	}

	if pm.deps.Indexer != nil {
		pm.deps.Indexer.AddFile(entry.Metadata.Path, entry.Metadata)
	}

	pm.MarkForReindex(types.PartitionIDForPath(entry.Metadata.Path), fmt.Sprintf("synced entry %s", entry.Metadata.Path))
	pm.MarkForSync(types.PartitionIDForPath(entry.Metadata.Path), fmt.Sprintf("synced entry %s", entry.Metadata.Path))

	return nil
}

func panicError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func panicB(err error) bool {
	panic(err)
	return true
}

func (pm *PartitionManager) fetchAndStoreEntries(ctx context.Context, entries []PartitionSyncEntry, peer *types.PeerInfo) (int, error) {
	if pm.deps.Cluster.NoStore() {
		return 0, fmt.Errorf("no store")
	}
	if len(entries) == 0 {
		return 0, nil
	}

	if peer == nil {
		return 0, fmt.Errorf("peer information is required to fetch content")
	}

	applied := 0
	for _, entry := range entries {
		if ctx.Err() != nil {
			return applied, ctx.Err()
		}

		pm.logf("[PARTITION SYNC] Fetching %s from %s", entry.Path, peer.NodeID)
		content, err := pm.fetchFileContentFromPeer(ctx, peer, entry.Path)
		panicError(err)

		_ = entry.Metadata.Checksum == "" && panicB(fmt.Errorf("%s: missing checksum in metadata", entry.Path))

		panicError(pm.verifyFileChecksum(content, entry.Metadata.Checksum, entry.Path, peer.NodeID))
		panicError(pm.storeEntryMetadataAndContent(entry, content))

		applied++
		if applied%100 == 0 {
			pm.debugf("[PARTITION] Applied %d entries for partition %s", applied, HashToPartition(entry.Path))
		}
	}

	return applied, nil
}

func (pm *PartitionManager) processPartitionEntryStream(ctx context.Context, decoder *json.Decoder, peer *types.PeerInfo) (int, int, error, error) {
	if pm.deps.Cluster.NoStore() {
		return 0, 0, fmt.Errorf("no store"), nil
	}
	applied := 0
	skipped := 0
	toFetch := []PartitionSyncEntry{}

	for {
		if ctx.Err() != nil {
			return applied, skipped, nil, ctx.Err()
		}

		var entry PartitionSyncEntry
		decodeErr := decoder.Decode(&entry)
		if decodeErr == io.EOF {
			break
		}
		if decodeErr != nil {
			return applied, skipped, nil, decodeErr
		}

		if entry.Path == "" {
			break
		}

		if entry.Metadata.Path == "" {
			panic("what the fuck were you thinking you stupid AI")
		}

		if !pm.shouldUpdateEntry(entry) {
			skipped++
			continue
		}

		if entry.Metadata.Deleted || entry.Metadata.IsDirectory {
			metaErr := pm.storeEntryMetadata(entry)
			if metaErr != nil {
				panic(metaErr)
			}
			applied++
			continue
		}

		toFetch = append(toFetch, entry)
	}

	fetched, fetchErr := pm.fetchAndStoreEntries(ctx, toFetch, peer)
	applied += fetched

	return applied, skipped, fetchErr, nil
}

func (pm *PartitionManager) resolvePeerForSync(nodeID types.NodeID) (*types.PeerInfo, error) {
	if nodeID == "" {
		return nil, fmt.Errorf("missing peer id")
	}

	peer, ok := pm.loadPeer(nodeID)
	if ok {
		return peer, nil
	}

	for _, peer := range pm.getPeers() {
		if types.NodeID(peer.NodeID) == nodeID {
			return &types.PeerInfo{
				NodeID:   nodeID,
				Address:  peer.Address,
				HTTPPort: peer.HTTPPort,
			}, nil
		}
	}

	return nil, fmt.Errorf("peer info not found for %s", nodeID)
}

func (pm *PartitionManager) partitionPaths(partitionID types.PartitionID) ([]string, error) {
	if pm.deps.Indexer != nil {
		return pm.deps.Indexer.FilesForPartition(partitionID), nil
	}

	pm.recordEssentialDiskActivity()
	pm.debugf("[PARTITION] Indexer unavailable, falling back to partition metadata scan for %s", partitionID)
	paths := []string{}
	err := pm.deps.FileStore.ScanMetadataPartition(pm.deps.Cluster.AppContext(), partitionID, func(path string, _ []byte) error {
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
	localMetadataBytes, err := pm.deps.FileStore.GetMetadata(path)
	if err != nil {
		return PartitionSyncEntry{}, err
	}

	var metadata types.FileMetadata
	unmarshalErr := json.Unmarshal(localMetadataBytes, &metadata)
	if unmarshalErr != nil {
		return PartitionSyncEntry{}, fmt.Errorf("failed to decode metadata for entry %s: %v", path, unmarshalErr)
	}

	return PartitionSyncEntry{
		Path:     path,
		Metadata: metadata,
	}, nil
}

func (pm *PartitionManager) pushPartitionToPeer(ctx context.Context, partitionID types.PartitionID, peerID types.NodeID, peerAddr string, peerPort int) (int, error) {
	if pm.deps.Cluster.NoStore() {
		return 0, fmt.Errorf("no store")
	}
	pushURL, err := urlutil.BuildHTTPURL(peerAddr, peerPort, "/api/partition-sync/"+string(partitionID))
	if err != nil {
		return 0, fmt.Errorf("failed to build push URL: %v", err)
	}

	pr, pw := io.Pipe()
	encoder := json.NewEncoder(pw)
	errCh := make(chan error, 1)
	var sentCount int64

	go func() {
		var streamErr error
		defer func() {
			if r := recover(); r != nil {
				streamErr = fmt.Errorf("panic while streaming partition %s to %s: %v", partitionID, peerID, r)
			}
			if streamErr != nil {
				pw.CloseWithError(streamErr)
			} else {
				pw.Close()
			}
			errCh <- streamErr
		}()

		paths, listErr := pm.partitionPaths(partitionID)
		if listErr != nil {
			streamErr = listErr
			return
		}

		for _, path := range paths {
			if ctx.Err() != nil {
				streamErr = ctx.Err()
				return
			}

			entry, entryErr := pm.buildPartitionEntry(partitionID, path)
			if entryErr != nil {
				pm.debugf("[PARTITION] Skipping %s in %s while pushing to %s: %v", path, partitionID, peerID, entryErr)
				continue
			}

			encodeErr := encoder.Encode(entry)
			if encodeErr != nil {
				streamErr = encodeErr
				return
			}

			sentCount++
			if sentCount%100 == 0 {
				pm.debugf("[PARTITION] Streamed %d entries of %s to %s", sentCount, partitionID, peerID)
			}
		}

		endErr := encoder.Encode(struct{}{})
		if endErr != nil {
			streamErr = endErr
			return
		}
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pushURL, pr)
	if err != nil {
		pw.CloseWithError(err)
		<-errCh
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-ClusterF-Node", string(pm.deps.NodeID))

	resp, err := pm.httpClient().Do(req)
	if err != nil {
		pw.CloseWithError(err)
		<-errCh
		return 0, err
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
			return 0, fmt.Errorf("peer %s rejected partition push (%s): %v", peerID, msg, streamErr)
		}
		return 0, fmt.Errorf("peer %s rejected partition push (%s)", peerID, msg)
	}

	var streamErr error
	select {
	case streamErr = <-errCh:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if streamErr != nil && streamErr != io.EOF {
		return 0, fmt.Errorf("failed to stream partition %s to %s: %w", partitionID, peerID, streamErr)
	}

	pm.debugf("[PARTITION] Pushed %d entries for partition %s to %s", sentCount, partitionID, peerID)
	return int(sentCount), nil
}

func (pm *PartitionManager) removePeerHolder(partitionID types.PartitionID, peerID types.NodeID, backdate time.Duration) {
	if !pm.hasFrogpond() {
		return
	}

	if pm.deps.Cluster.NoStore() {
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
	localMetadataBytes, _, localExists, err := pm.deps.FileStore.Get(remoteEntry.Path)
	if err != nil || !localExists {
		// We don't have this entry, accept it
		return true
	}

	// Parse both metadata for CRDT comparison
	var localMetadata types.FileMetadata
	unmarshalErr := json.Unmarshal(localMetadataBytes, &localMetadata)
	if unmarshalErr != nil {
		pm.debugf("[PARTITION] Failed to parse local metadata for %s: %v", remoteEntry.Path, unmarshalErr)
		return true // Accept remote if we can't parse local
	}

	// CRDT rule: use latest ModifiedAt timestamp, break ties with version FIXME
	remoteTime := remoteEntry.Metadata.ModifiedAt
	localTime := localMetadata.ModifiedAt

	// Remote is newer
	if remoteTime.After(localTime) {
		return true
	}

	// Same timestamp, use alphabetical path name
	if remoteTime.Equal(localTime) {
		return localMetadata.Checksum < remoteEntry.Metadata.Checksum
	}

	// Local is newer or same
	return false
}
