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

	go func() {
		peers := c.GetAvailablePeerList()
		for _, peer := range peers {
			if c.AppContext().Err() != nil {
				return
			}
			func(p types.NodeData) {
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
				defer func() {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}()
			}(peer)
		}
	}()
}

// publishMetricsDataPoint writes metrics snapshots into frogpond and propagates them
func (c *Cluster) publishMetricsDataPoint(key string, payload []byte) {
	if c.frogpond == nil {
		return
	}

	updates := c.frogpond.SetDataPoint(key, payload)
	if len(updates) > 0 {
		c.sendUpdatesToPeers(updates)
	}
}

func (c *Cluster) getPeerList() []types.NodeData {
	nodes := c.frogpond.GetAllMatchingPrefix("nodes/")
	var peerList []types.NodeData

	for _, nodeDataPoint := range nodes {
		if nodeDataPoint.Deleted {
			continue
		}

		var nodeData types.NodeData
		if err := json.Unmarshal(nodeDataPoint.Value, &nodeData); err != nil {
			continue
		}

		peerList = append(peerList, nodeData)
	}

	return peerList
}

func (c *Cluster) GetAvailablePeerList() []types.NodeData {
	nodes := c.getPeerList()
	peers := c.DiscoveryManager().GetPeerMap()
	var availablePeerList []types.NodeData
	fmt.Printf("Considering %+v nodes and %+v peers\n", nodes, peers)

	for _, nodeData := range nodes {
		fmt.Printf("Considering node %+v\n", nodeData)
		if nodeData.IsStorage {
			fmt.Printf("Considering storage node %+v\n", nodeData)
			if nodeData.DiskSize > 0 {
				used := nodeData.DiskSize - nodeData.DiskFree
				if used < 0 {
					used = 0
				}
				usage := float64(used) / float64(nodeData.DiskSize)
				if usage >= 0.9 {
					fmt.Printf("Skipping node %+v (disk usage %v)\n", nodeData, usage)
					continue
				}
			}

			discPeer, ok := peers.Load(string(nodeData.NodeID))
			if !ok {
				fmt.Printf("Skipping node %+v (no peer)\n", nodeData)
				continue
			}
			nodeData.Address = discPeer.Address
			nodeData.HTTPPort = discPeer.HTTPPort

			fmt.Printf("Adding node %+v\n", nodeData)
			availablePeerList = append(availablePeerList, nodeData)
		} else {
			fmt.Printf("Skipping node %+v (not storage)\n", nodeData)
		}

	}
	fmt.Printf("Returning %+v\n", availablePeerList)
	return availablePeerList
}

func (c *Cluster) GetAvailablePeerMap() map[types.NodeID]types.NodeData {
	out := make(map[types.NodeID]types.NodeData)
	nodes := c.GetAvailablePeerList()
	for _, nodeData := range nodes {
		out[types.NodeID(nodeData.NodeID)] = nodeData
	}
	return out
}

// GetCurrentRF gets the current replication factor from frogpond
func (c *Cluster) GetCurrentRF() int {
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

// SetPartitionSyncInterval updates the cluster partition sync interval (in seconds)
func (c *Cluster) SetPartitionSyncInterval(seconds int) {
	if seconds < 1 {
		seconds = 1
	}

	intervalJSON, _ := json.Marshal(seconds)
	updates := c.frogpond.SetDataPoint("cluster/partition_sync_interval_seconds", intervalJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[PARTITION] Set partition sync interval to %d seconds", seconds)
}

// GetPartitionSyncInterval returns the current partition sync interval in seconds.
func (c *Cluster) GetPartitionSyncInterval() int {
	if c.frogpond == nil {
		return types.DefaultPartitionSyncIntervalSeconds
	}

	dp := c.frogpond.GetDataPoint("cluster/partition_sync_interval_seconds")
	if dp.Deleted || len(dp.Value) == 0 {
		return types.DefaultPartitionSyncIntervalSeconds
	}

	var seconds int
	if err := json.Unmarshal(dp.Value, &seconds); err != nil {
		return types.DefaultPartitionSyncIntervalSeconds
	}

	if seconds < 1 {
		return 1
	}
	return seconds
}

// GetPartitionSyncPaused returns whether partition sync is paused
func (c *Cluster) GetPartitionSyncPaused() bool {
	data := c.frogpond.GetDataPoint("cluster/partition_sync_paused")
	if data.Deleted || len(data.Value) == 0 {
		return false
	}

	var paused bool
	if err := json.Unmarshal(data.Value, &paused); err != nil {
		return false
	}

	return paused
}

// setPartitionSyncPaused updates the cluster partition sync pause state
func (c *Cluster) setPartitionSyncPaused(paused bool) {
	pausedJSON, _ := json.Marshal(paused)
	updates := c.frogpond.SetDataPoint("cluster/partition_sync_paused", pausedJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[PARTITION] Set partition sync paused to %v", paused)
}

// GetSleepMode returns whether sleep mode is enabled
func (c *Cluster) GetSleepMode() bool {
	data := c.frogpond.GetDataPoint("cluster/sleep_mode")
	if data.Deleted || len(data.Value) == 0 {
		return false
	}

	var sleepMode bool
	if err := json.Unmarshal(data.Value, &sleepMode); err != nil {
		return false
	}

	return sleepMode
}

// setSleepMode updates the cluster sleep mode state
func (c *Cluster) setSleepMode(sleepMode bool) {
	sleepModeJSON, _ := json.Marshal(sleepMode)
	updates := c.frogpond.SetDataPoint("cluster/sleep_mode", sleepModeJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[SLEEP_MODE] Set sleep mode to %v", sleepMode)
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

// setClusterRestart sets the cluster restart task with current timestamp
func (c *Cluster) setClusterRestart() {
	restartTime := time.Now().Unix() // Unix timestamp in seconds
	restartJSON, _ := json.Marshal(restartTime)
	updates := c.frogpond.SetDataPoint("tasks/restart", restartJSON)
	c.sendUpdatesToPeers(updates)

	c.Logger().Printf("[RESTART] Set cluster restart task with timestamp %d", restartTime)
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
	targetSet := make(map[types.NodeID]bool)
	for _, dp := range dataPoints {
		if dp.Deleted || len(dp.Value) == 0 {
			continue
		}

		// Extract node ID from key
		nodeID := types.NodeID(strings.TrimPrefix(string(dp.Key), holderPrefix))

		if nodeID == "" {
			panic("no")
		}
		if targetSet[nodeID] {
			continue
		}
		targetSet[nodeID] = true

	}

	for nodeID := range targetSet {
		holders = append(holders, nodeID)
	}

	return holders
}

// GetNodesForPartition returns nodes that hold a specific partition
func (c *Cluster) GetNodesForPartitionMap(partitionName string) []types.NodeID {
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

		var nodeData types.NodeData
		if err := json.Unmarshal(data.Value, &nodeData); err != nil {
			continue
		}

		if nodeData.Available {
			if nodeData.NodeID != "" {
				available = append(available, types.NodeID(nodeData.NodeID))
			}
		}
	}

	return available
}

// periodicFrogpondSync regularly updates our node metadata
func (c *Cluster) periodicFrogpondSync(ctx context.Context) {
	c.updateNodeMetadata()
	c.persistCRDTToFile()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastUpdate time.Time

	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Update our node metadata
			c.updateNodeMetadata()
			c.logger.Print("Updated NodeMetadata\n")
			if time.Since(lastUpdate) < time.Duration(c.GetPartitionSyncInterval()) {
				continue
			}

			// Persist CRDT state to KV
			c.persistCRDTToFile()
			c.logger.Print("Wrote persistCRDTToFile to disk\n")
			lastUpdate = time.Now()
		}

	}
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
	metrics := c.loadDiskMetrics()
	diskSize := metrics.diskSize
	diskFree := metrics.diskFree

	if c.CanRunNonEssentialDiskOp() {
		diskSize, diskFree = c.getDiskUsage()
		c.recordDiskMetrics(0, diskSize, diskFree)
	} else {
		c.debugf("[DISK_ACTIVITY] Disk inactive; using cached disk metrics for metadata update")
	}

	syncPending := 0
	reindexPending := 0
	if c.partitionManager != nil {
		syncPending = c.partitionManager.SyncListPendingCount()
		reindexPending = c.partitionManager.ReindexListPendingCount()
	}

	// Get our external address from discovery
	address := ""
	peers := c.DiscoveryManager().GetPeers()
	for _, peer := range peers {
		if peer.NodeID == c.NodeId {
			address = peer.Address
			break
		}
	}

	// Load storage settings for enhanced metadata
	storageSettings, _ := loadStorageSettings(c.DataDir)
	storageFormat := "unknown"
	storageMinor := ""
	program := "clusterF"
	version := "unknown"
	url := "https://github.com/donomii/clusterF"
	if storageSettings != nil {
		storageFormat = storageSettings.StorageMajor
		storageMinor = storageSettings.StorageMinor
		program = storageSettings.Program
		version = storageSettings.Version
		url = storageSettings.URL
	}

	// Get absolute path for data directory for THIS node only
	absDataDir, _ := filepath.Abs(c.DataDir)

	nodeData := types.NodeData{
		NodeID:         c.NodeId,
		Address:        address,
		HTTPPort:       c.HTTPDataPort,
		DiscoveryPort:  c.DiscoveryPort,
		LastSeen:       time.Now(),
		Available:      true,
		BytesStored:    0,
		DiskSize:       diskSize,
		DiskFree:       diskFree,
		SyncPending:    syncPending,
		ReindexPending: reindexPending,
		IsStorage:      !c.NoStore(),
		DataDir:        absDataDir,
		StorageFormat:  storageFormat,
		StorageMinor:   storageMinor,
		Program:        program,
		Version:        version,
		URL:            url,
		ExportDir:      c.ExportDir,
		ClusterDir:     c.ClusterDir,
		ImportDir:      c.ImportDir,
		Debug:          c.Debug,
	}
	nodeJSON, _ := json.Marshal(nodeData)
	updates := c.frogpond.SetDataPoint(nodeKey, nodeJSON)
	c.sendUpdatesToPeers(updates)
}

// handleFrogpondUpdate handles incoming frogpond updates from peers
func (c *Cluster) handleFrogpondUpdate(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		c.Logger().Printf("[DEBUG] Method not allowed: %s", r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := types.ReadAll(r.Body)
	if err != nil {
		c.Logger().Printf("[DEBUG] Failed to read request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var updates []frogpond.DataPoint
	if err := json.Unmarshal(body, &updates); err != nil {
		c.Logger().Printf("[DEBUG] Failed to parse updates from %s: error=%v, body_len=%d, first_100_bytes=%q", r.RemoteAddr, err, len(body), string(body[:min(100, len(body))]))
		http.Error(w, "Failed to parse updates", http.StatusBadRequest)
		return
	}

	// Apply the updates to our frogpond and get any resulting updates to propagate
	resultingUpdates := c.frogpond.AppendDataPoints(updates)

	c.sendUpdatesToPeers(resultingUpdates)

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
	if !c.CanRunNonEssentialDiskOp() {
		c.debugf("[DISK_ACTIVITY] Skipping CRDT persistence; disk inactive")
		return
	}

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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastUpdate time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if time.Since(lastUpdate) > time.Duration(c.GetPartitionSyncInterval())*time.Second {
				c.logger.Printf("Started pruneOldNodes\n")
				c.pruneOldNodes()
				c.logger.Printf("Finished pruneOldNodes\n")
				lastUpdate = time.Now()
			}
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
	c.RecordDiskActivity(types.DiskActivityEssential)
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
