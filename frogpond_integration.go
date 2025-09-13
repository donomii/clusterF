// frogpond_integration.go - Additional methods for CRDT coordination
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/donomii/frogpond"
)

// updateChunkMetadata stores chunk location info in frogpond
func (c *Cluster) updateChunkMetadata(chunkID ChunkID, action string) {
	// Each node stores its own holder record
	holderKey := fmt.Sprintf("chunks/%s/holders/%s", chunkID, c.ID)

	switch action {
	case "add":
		// Add our holder record
		holderData := map[string]interface{}{
			"node_id":   string(c.ID),
			"timestamp": time.Now().Unix(),
			"active":    true,
		}
		holderJSON, _ := json.Marshal(holderData)
		updates1 := c.frogpond.SetDataPoint(holderKey, holderJSON)
		// Persist to KV
		if c.crdtKV != nil {
			_ = c.crdtKV.Put([]byte(holderKey), holderJSON)
		}
		c.sendUpdatesToPeers(updates1)

	case "remove":
		// Mark our holder record as inactive
		holderData := map[string]interface{}{
			"node_id":   string(c.ID),
			"timestamp": time.Now().Unix(),
			"active":    false,
		}
		holderJSON, _ := json.Marshal(holderData)
		updates1 := c.frogpond.SetDataPoint(holderKey, holderJSON)
		if c.crdtKV != nil {
			_ = c.crdtKV.Put([]byte(holderKey), holderJSON)
		}
		c.sendUpdatesToPeers(updates1)
	}

	// Store chunk metadata
	metaKey := fmt.Sprintf("chunks/%s/meta", chunkID)
	meta := map[string]interface{}{
		"chunk_id":  string(chunkID),
		"modified":  time.Now().Unix(),
		"rf_target": c.getCurrentRF(),
	}
	if action == "remove" {
		meta["deleted"] = true
		meta["delete_time"] = time.Now().Unix()
	}
	metaJSON, _ := json.Marshal(meta)
	updates2 := c.frogpond.SetDataPoint(metaKey, metaJSON)
	if c.crdtKV != nil {
		_ = c.crdtKV.Put([]byte(metaKey), metaJSON)
	}
	c.sendUpdatesToPeers(updates2)
}

// setHolderActive updates only the holder record for this node without touching global meta
func (c *Cluster) setHolderActive(chunkID ChunkID, active bool) {
	holderKey := fmt.Sprintf("chunks/%s/holders/%s", chunkID, c.ID)
	holderData := map[string]interface{}{
		"node_id":   string(c.ID),
		"timestamp": time.Now().Unix(),
		"active":    active,
	}
	holderJSON, _ := json.Marshal(holderData)
	updates := c.frogpond.SetDataPoint(holderKey, holderJSON)
	if c.crdtKV != nil {
		_ = c.crdtKV.Put([]byte(holderKey), holderJSON)
	}
	c.sendUpdatesToPeers(updates)
}

// getChunkHoldersFromFrogpond retrieves chunk holders from frogpond
func (c *Cluster) getChunkHoldersFromFrogpond(chunkID ChunkID) []NodeID {
	holderPrefix := fmt.Sprintf("chunks/%s/holders/", chunkID)
	holderData := c.frogpond.GetAllMatchingPrefix(holderPrefix)

	var activeHolders []NodeID
	for _, data := range holderData {
		if data.Deleted || len(data.Value) == 0 {
			continue
		}

		var holder map[string]interface{}
		if err := json.Unmarshal(data.Value, &holder); err != nil {
			continue
		}

		// Only include active holders
		if active, ok := holder["active"].(bool); ok && active {
			if nodeIDStr, ok := holder["node_id"].(string); ok {
				activeHolders = append(activeHolders, NodeID(nodeIDStr))
			}
		}
	}

	return activeHolders
}

// isChunkDeleted checks if chunk is tombstoned in frogpond
func (c *Cluster) isChunkDeleted(chunkID ChunkID) bool {
	metaKey := fmt.Sprintf("chunks/%s/meta", chunkID)
	data := c.frogpond.GetDataPoint(metaKey)

	if data.Deleted || len(data.Value) == 0 {
		return false
	}

	var meta map[string]interface{}
	if err := json.Unmarshal(data.Value, &meta); err != nil {
		return false
	}

	deleted, _ := meta["deleted"].(bool)
	return deleted
}

// sendUpdatesToPeers sends frogpond updates to all discovered peers
func (c *Cluster) sendUpdatesToPeers(updates []frogpond.DataPoint) {
	if len(updates) == 0 {
		return
	}

	peers := c.DiscoveryManager.GetPeers()
	for _, peer := range peers {
		func(p *PeerInfo) {
			url := fmt.Sprintf("http://%s:%d/frogpond/update", p.Address, p.HTTPPort)
			updatesJSON, _ := json.Marshal(updates)

			resp, err := c.httpClient.Post(url, "application/json", strings.NewReader(string(updatesJSON)))
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}(peer)
	}
}

// getCurrentRF gets the current replication factor from frogpond
func (c *Cluster) getCurrentRF() int {
	data := c.frogpond.GetDataPoint("cluster/replication_factor")
	if data.Deleted || len(data.Value) == 0 {
		return DefaultRF
	}

	var rf int
	if err := json.Unmarshal(data.Value, &rf); err != nil {
		return DefaultRF
	}

	if rf < 1 {
		return 1
	}
	return rf
}

// setReplicationFactor updates the cluster replication factor
func (c *Cluster) setReplicationFactor(rf int) {
	if rf < 1 {
		rf = 1
	}

	rfJSON, _ := json.Marshal(rf)
	updates := c.frogpond.SetDataPoint("cluster/replication_factor", rfJSON)
	if c.crdtKV != nil {
		_ = c.crdtKV.Put([]byte("cluster/replication_factor"), rfJSON)
	}
	c.sendUpdatesToPeers(updates)

	c.Logger.Printf("[RF] Set replication factor to %d", rf)

}

// handleOverReplication removes excess chunk copies when RF is reduced
func (c *Cluster) handleOverReplication(chunkID ChunkID, holders []NodeID, targetRF int) {
	excessReplicas := len(holders) - targetRF
	if excessReplicas <= 0 {
		return
	}

	// Only remove our copy if we're not the only holder and we have excess
	haveChunk := c.HasChunk(chunkID)
	if haveChunk && len(holders) > targetRF {
		// Check if we should remove our copy (simple heuristic: remove if we're in the "excess" portion)
		ourPosition := -1
		for i, holder := range holders {
			if holder == c.ID {
				ourPosition = i
				break
			}
		}

		// Remove our copy if we're beyond the target RF position
		if ourPosition >= targetRF {
			c.DeleteLocalChunk(chunkID)
			c.updateChunkMetadata(chunkID, "remove")
			c.Logger.Printf("[RF_ADJUST] Removed excess replica of %s (RF reduced to %d)", chunkID, targetRF)
		}
	}
}

// getAllChunksNeedingReplication scans frogpond for under-replicated chunks
func (c *Cluster) getAllChunksNeedingReplication() []ChunkID {
	chunks := c.frogpond.GetAllMatchingPrefix("chunks/")
	var underReplicated []ChunkID
	currentRF := c.getCurrentRF()

	processedChunks := make(map[string]bool)

	for _, data := range chunks {
		if data.Deleted {
			continue
		}

		// Extract chunk ID from key (format: chunks/{id}/meta or chunks/{id}/holders/{nodeID})
		keyParts := strings.Split(string(data.Key), "/")
		if len(keyParts) < 2 {
			continue
		}
		chunkIDStr := keyParts[1]

		// Skip if we've already processed this chunk
		if processedChunks[chunkIDStr] {
			continue
		}
		processedChunks[chunkIDStr] = true

		chunkID := ChunkID(chunkIDStr)

		// Check if chunk is deleted
		if c.isChunkDeleted(chunkID) {
			continue
		}

		// Get holders
		holders := c.getChunkHoldersFromFrogpond(chunkID)
		if len(holders) < currentRF {
			underReplicated = append(underReplicated, chunkID)
		}
	}

	return underReplicated
}

// getUnderReplicatedCount returns the count of under-replicated chunks
func (c *Cluster) getUnderReplicatedCount() int {
	return len(c.getAllChunksNeedingReplication())
}

// getAvailableNodes returns list of available nodes from frogpond
func (c *Cluster) getAvailableNodes() []NodeID {
	nodes := c.frogpond.GetAllMatchingPrefix("nodes/")
	var available []NodeID

	for _, data := range nodes {
		if data.Deleted {
			continue
		}

		var nodeData map[string]interface{}
		if err := json.Unmarshal(data.Value, &nodeData); err != nil {
			continue
		}

		if avail, ok := nodeData["available"].(bool); ok && avail {
			if nodeIDStr, ok := nodeData["node_id"].(string); ok {
				available = append(available, NodeID(nodeIDStr))
			}
		}
	}

	return available
}

// periodicFrogpondSync regularly updates our node metadata
func (c *Cluster) periodicFrogpondSync(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update our node metadata
			c.updateNodeMetadata()
		}
	}
}

// updateNodeMetadata stores this node's metadata in frogpond
func (c *Cluster) updateNodeMetadata() {
	nodeKey := fmt.Sprintf("nodes/%s", c.ID)
	nodeData := map[string]interface{}{
		"node_id":   string(c.ID),
		"http_port": c.HTTPDataPort,
		"last_seen": time.Now().Unix(),
		"available": true,
	}
	nodeJSON, _ := json.Marshal(nodeData)
	updates := c.frogpond.SetDataPoint(nodeKey, nodeJSON)
	c.sendUpdatesToPeers(updates)
}

// handleFrogpondUpdate handles incoming frogpond updates from peers
func (c *Cluster) handleFrogpondUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var updates []frogpond.DataPoint
	if err := json.Unmarshal(body, &updates); err != nil {
		http.Error(w, "Failed to parse updates", http.StatusBadRequest)
		return
	}

	// Apply the updates to our frogpond and get any resulting updates to propagate
	resultingUpdates := c.frogpond.AppendDataPoints(updates)
	c.sendUpdatesToPeers(resultingUpdates)

	w.WriteHeader(http.StatusOK)
}

// handleFrogpondFullSync handles incoming full store sync from a new peer
func (c *Cluster) handleFrogpondFullSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var peerData []frogpond.DataPoint
	if err := json.Unmarshal(body, &peerData); err != nil {
		http.Error(w, "Failed to parse full store data", http.StatusBadRequest)
		return
	}

	// Apply the peer's full store and get any resulting updates
	resultingUpdates := c.frogpond.AppendDataPoints(peerData)
	c.sendUpdatesToPeers(resultingUpdates)

	c.Logger.Printf("[FULL_SYNC] Received and applied %d data points from peer", len(peerData))
	w.WriteHeader(http.StatusOK)
}

// handleFrogpondFullStore serves our complete frogpond store to requesting peers
func (c *Cluster) handleFrogpondFullStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Return our complete frogpond store as JSON
	w.Header().Set("Content-Type", "application/json")
	w.Write(c.frogpond.JsonDump())
}
