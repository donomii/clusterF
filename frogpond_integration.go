// frogpond_integration.go - Additional methods for CRDT coordination
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/donomii/clusterF/types"
	"github.com/donomii/clusterF/urlutil"
	"github.com/donomii/frogpond"
	"golang.org/x/sys/unix"
)

// sendUpdatesToPeers sends frogpond updates to all discovered peers
func (c *Cluster) sendUpdatesToPeers(updates []frogpond.DataPoint) {
	if len(updates) == 0 {
		return
	}

	peers := c.DiscoveryManager().GetPeers()
	for _, peer := range peers {
		func(p *types.PeerInfo) {
			endpointURL, err := urlutil.BuildHTTPURL(p.Address, p.HTTPPort, "/frogpond/update")
			if err != nil {
				c.debugf("[FROGPOND] Failed to build update URL for %s: %v", p.NodeID, err)
				return
			}
			updatesJSON, _ := json.Marshal(updates)

			resp, err := c.httpClient.Post(endpointURL, "application/json", strings.NewReader(string(updatesJSON)))
			if err != nil {
				return
			}
			defer resp.Body.Close()
		}(peer)
	}
}

func (c *Cluster) getPeerList() []types.PeerInfo {
	nodes := c.frogpond.GetAllMatchingPrefix("nodes/")
	var peerList []types.PeerInfo

	for _, nodeDataPoint := range nodes {
		if nodeDataPoint.Deleted {
			continue
		}

		var nodeData types.NodeData
		if err := json.Unmarshal(nodeDataPoint.Value, &nodeData); err != nil {
			continue
		}

		peer := types.PeerInfo{
			NodeID:      nodeData.NodeID,
			Address:     nodeData.Address,
			HTTPPort:    nodeData.HTTPPort,
			LastSeen:    time.Unix(nodeData.LastSeen, 0),
			Available:   nodeData.Available,
			BytesStored: nodeData.BytesStored,
			DiskSize:    nodeData.DiskSize,
			DiskFree:    nodeData.DiskFree,
			IsStorage:   nodeData.IsStorage,
		}

		peerList = append(peerList, peer)
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

// getPartitionSyncInterval gets the partition sync interval from frogpond (in seconds)
func (c *Cluster) getPartitionSyncInterval() int {
	data := c.frogpond.GetDataPoint("cluster/partition_sync_interval")
	if data.Deleted || len(data.Value) == 0 {
		return 30 // Default 30 seconds
	}

	var interval int
	if err := json.Unmarshal(data.Value, &interval); err != nil {
		return 30
	}

	if interval < 1 {
		return 1
	}
	return interval
}

// setPartitionSyncInterval updates the cluster partition sync interval (in seconds)
func (c *Cluster) setPartitionSyncInterval(seconds int) {
	if seconds < 1 {
		seconds = 1
	}

	intervalJSON, _ := json.Marshal(seconds)
	updates := c.frogpond.SetDataPoint("cluster/partition_sync_interval_seconds", intervalJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[PARTITION] Set partition sync interval to %d seconds", seconds)
}

// setReplicationFactor updates the cluster replication factor
func (c *Cluster) setReplicationFactor(rf int) {
	if rf < 1 {
		rf = 1
	}

	rfJSON, _ := json.Marshal(rf)
	updates := c.frogpond.SetDataPoint("cluster/replication_factor", rfJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[RF] Set replication factor to %d", rf)

}

// GetAllNodes returns all nodes from CRDT
func (c *Cluster) GetAllNodes() map[types.NodeID]*types.NodeData {
	nodes := c.frogpond.GetAllMatchingPrefix("nodes/")
	allNodes := make(map[types.NodeID]*types.NodeData)

	for _, data := range nodes {
		if data.Deleted {
			continue
		}

		var nodeData types.NodeData
		if err := json.Unmarshal(data.Value, &nodeData); err != nil {
			continue
		}

		allNodes[types.NodeID(nodeData.NodeID)] = &nodeData
	}

	return allNodes
}

// GetNodesForPartition returns nodes that hold a specific partition
func (c *Cluster) GetNodesForPartition(partitionName string) []types.NodeID {
	holderPrefix := fmt.Sprintf("partitions/%s/holders/", partitionName)
	dataPoints := c.frogpond.GetAllMatchingPrefix(holderPrefix)

	var holders []types.NodeID
	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Extract node ID from key
		nodeID := strings.TrimPrefix(string(dp.Key), holderPrefix)
		holders = append(holders, types.NodeID(nodeID))
	}

	return holders
}

// GetNodeInfo returns information about a specific node
func (c *Cluster) GetNodeInfo(nodeID types.NodeID) *types.NodeData {
	nodeKey := fmt.Sprintf("nodes/%s", nodeID)
	dp := c.frogpond.GetDataPoint(nodeKey)

	if dp.Deleted || len(dp.Value) == 0 {
		return nil
	}

	var nodeData types.NodeData
	if err := json.Unmarshal(dp.Value, &nodeData); err != nil {
		return nil
	}

	return &nodeData
}

// getAvailableNodes returns list of available nodes from frogpond
func (c *Cluster) getAvailableNodes() []types.NodeID {
	nodes := c.frogpond.GetAllMatchingPrefix("nodes/")
	var available []types.NodeID

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
				available = append(available, types.NodeID(nodeIDStr))
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

// calculateDataDirSize calculates the total size of all files in the data directory
func (c *Cluster) calculateDataDirSize() int64 {
	var totalSize int64
	err := filepath.Walk(c.DataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files with errors
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		c.debugf("[DISK_USAGE] Error calculating data directory size: %v", err)
		return 0
	}
	return totalSize
}

// getDiskUsage gets disk size and free space for the data directory
func (c *Cluster) getDiskUsage() (diskSize int64, diskFree int64) {
	var stat unix.Statfs_t
	err := unix.Statfs(c.DataDir, &stat)
	if err != nil {
		c.debugf("[DISK_USAGE] Error getting disk stats for %s: %v", c.DataDir, err)
		return 0, 0
	}

	// Calculate total size and free space in bytes
	diskSize = int64(stat.Blocks) * int64(stat.Bsize)
	diskFree = int64(stat.Bavail) * int64(stat.Bsize)

	return diskSize, diskFree
}

// updateNodeMetadata stores this node's metadata in frogpond
func (c *Cluster) updateNodeMetadata() {
	nodeKey := fmt.Sprintf("nodes/%s", c.NodeId)

	// Calculate disk usage information
	bytesStored := c.calculateDataDirSize()
	diskSize, diskFree := c.getDiskUsage()

	// Get our external address from discovery
	address := ""
	peers := c.DiscoveryManager().GetPeers()
	for _, peer := range peers {
		if peer.NodeID == string(c.NodeId) {
			address = peer.Address
			break
		}
	}

	nodeData := types.NodeData{
		NodeID:      string(c.NodeId),
		Address:     address,
		HTTPPort:    c.HTTPDataPort,
		LastSeen:    time.Now().Unix(),
		Available:   true,
		BytesStored: bytesStored,
		DiskSize:    diskSize,
		DiskFree:    diskFree,
		IsStorage:   !c.noStore,
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

	c.Logger().Printf("[FULL_SYNC] Received and applied %d data points from peer", len(peerData))
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
		c.Logger().Printf("Failed to marshal CRDT data for persistence: %v", err)
		return
	}
	ioutil.WriteFile(filepath.Join(c.DataDir, "crdt_backup.json"), dataJSON, 0644)
}

// periodicNodePruning creates backdated tombstones for all node entries every 10 minutes
// This forces nodes to re-apply their keys or be removed from the cluster
func (c *Cluster) periodicNodePruning(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.pruneOldNodes()
		}
	}
}

// pruneOldNodes creates backdated tombstones for all node entries
// Tombstones are backdated by 30 minutes so active nodes will override them
// but nodes that have been gone for >30 minutes will be removed
func (c *Cluster) pruneOldNodes() {
	c.Logger().Printf("[NODE_PRUNING] Starting periodic node pruning")

	// Get all node keys
	nodeKeys := c.frogpond.GetAllMatchingPrefix("nodes/")

	// Create backdated tombstones for all node entries
	backdateTime := time.Now().Add(-30 * time.Minute)
	tombstones := []frogpond.DataPoint{}

	for _, nodeData := range nodeKeys {
		// Skip if already deleted
		if nodeData.Deleted {
			continue
		}

		// Create a backdated tombstone
		tombstone := frogpond.DataPoint{
			Key:     nodeData.Key,
			Value:   nil,
			Name:    nodeData.Name,
			Updated: backdateTime,
			Deleted: true,
		}

		tombstones = append(tombstones, tombstone)
	}

	if len(tombstones) > 0 {
		c.Logger().Printf("[NODE_PRUNING] Created %d backdated tombstones for node entries", len(tombstones))

		// Apply the tombstones to our own CRDT and get resulting updates
		resultingUpdates := c.frogpond.AppendDataPoints(tombstones)

		// Send the tombstones to all peers
		c.sendUpdatesToPeers(tombstones)

		// Also send any resulting updates (in case some nodes were actually pruned)
		if len(resultingUpdates) > 0 {
			c.sendUpdatesToPeers(resultingUpdates)
			c.Logger().Printf("[NODE_PRUNING] Pruned %d stale node entries", len(resultingUpdates))
		}
	} else {
		c.Logger().Printf("[NODE_PRUNING] No node entries found to prune")
	}
}

// loadCRDTFromKV seeds the in-memory CRDT from the persistent KV
func (c *Cluster) loadCRDTFromFile() {
	data, err := ioutil.ReadFile(filepath.Join(c.DataDir, "crdt_backup.json"))
	if err != nil {
		c.Logger().Printf("Could not read CRDT backup file: %v", err)
		return
	}
	var allData []frogpond.DataPoint
	if err := json.Unmarshal(data, &allData); err != nil {
		c.Logger().Printf("Could not unmarshal CRDT backup data: %v", err)
		return
	}
	c.frogpond.AppendDataPoints(allData)
	c.Logger().Printf("Loaded CRDT from backup file")
}
