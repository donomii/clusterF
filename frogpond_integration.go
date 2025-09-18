// frogpond_integration.go - Additional methods for CRDT coordination
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/donomii/frogpond"
)

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

func (c *Cluster) getPeerList() []PeerInfo {
	peers := c.DiscoveryManager.GetPeers()
	var peerList []PeerInfo
	for _, p := range peers {
		peerList = append(peerList, *p)
	}
	return peerList
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
	if c.contentKV != nil {
		_ = c.contentKV.Put([]byte("cluster/replication_factor"), rfJSON)
	}
	c.sendUpdatesToPeers(updates)

	c.Logger.Printf("[RF] Set replication factor to %d", rf)

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
			// Persist CRDT state to KV
			c.persistCRDTToFile()
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

// persistCRDTToFile saves the current CRDT state to a file
func (c *Cluster) persistCRDTToFile() {
	dataPoints := c.frogpond.DataPool.ToList()
	dataJSON, err := json.Marshal(dataPoints)
	if err != nil {
		c.Logger.Printf("Failed to marshal CRDT data for persistence: %v", err)
		return
	}
	ioutil.WriteFile(filepath.Join(c.DataDir, "crdt_backup.json"), dataJSON, 0644)
}
