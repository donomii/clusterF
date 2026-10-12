// discovery.go - Auto-discovery and peer networking
package discovery

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/threadmanager"
	"github.com/donomii/clusterF/types"
)

// UpdateNodeInfo updates our node's information (e.g., when HTTP port changes)
func (dm *DiscoveryManager) UpdateNodeInfo(nodeID string, httpPort int) {
	if nodeID == dm.nodeID {
		// Update our own HTTP port
		dm.mu.Lock()
		oldPort := dm.httpPort
		dm.httpPort = httpPort
		dm.mu.Unlock()
		if oldPort != httpPort {
			dm.logger.Printf("HTTP port updated from %d to %d, broadcasting new info", oldPort, httpPort)
			// Send immediate broadcast with new port info
			dm.broadcast()
		}
	}
}

// DiscoveryManager handles auto-discovery of cluster peers
type DiscoveryManager struct {
	mu            sync.RWMutex
	nodeID        string
	httpPort      int
	broadcastPort int
	logger        *log.Logger
	debug         bool
	mode          string

	// Peer tracking
	peers *syncmap.SyncMap[string, *types.PeerInfo]

	// Network components
	broadcastConn *net.UDPConn
	listenConn    *net.UDPConn

	// Configuration
	broadcastInterval time.Duration
	peerTimeout       time.Duration

	// Thread management
	threadManager *threadmanager.ThreadManager
}

// NewDiscoveryManager creates a new discovery manager
func NewDiscoveryManager(nodeID string, httpPort int, discoveryPort int, tm *threadmanager.ThreadManager, logger *log.Logger) *DiscoveryManager {
	if discoveryPort == 0 {
		discoveryPort = 9999 // Default discovery port
	}
	mode := os.Getenv("CLUSTER_DISCOVERY_MODE")
	if mode == "" {
		mode = "udp"
	}
	return &DiscoveryManager{
		nodeID:            nodeID,
		httpPort:          httpPort,
		broadcastPort:     discoveryPort,
		logger:            logger,
		peers:             syncmap.NewSyncMap[string, *types.PeerInfo](),
		broadcastInterval: 5 * time.Second,
		peerTimeout:       30 * time.Second, // The peer timeout must be larger than the peer rebroadcast
		threadManager:     tm,
		debug:             false,
		mode:              mode,
	}
}

// Debugf logs a debug message if debug is enabled
func (dm *DiscoveryManager) Debugf(format string, v ...interface{}) {
	if dm.debug {
		dm.logger.Printf(format, v...)
	}
}

// SetTimings configures broadcast and timeout intervals
func (dm *DiscoveryManager) SetTimings(broadcastInterval, peerTimeout time.Duration) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.broadcastInterval = broadcastInterval
	dm.peerTimeout = peerTimeout
}

// Start begins the discovery process
func (dm *DiscoveryManager) Start() error {
	//FIXME:  Put this in a loop so we handle network changes
	dm.Debugf("Starting discovery on port %d", dm.broadcastPort)

	if err := dm.openSockets(); err != nil {
		return err
	}

	// Start background threads
	if err := dm.threadManager.StartThread("discovery-broadcast", dm.broadcastLoop); err != nil {
		dm.Stop()
		return fmt.Errorf("failed to start broadcast thread: %w", err)
	}

	if err := dm.threadManager.StartThread("discovery-listen", dm.listenLoop); err != nil {
		dm.Stop()
		return fmt.Errorf("failed to start listen thread: %w", err)
	}

	if err := dm.threadManager.StartThread("discovery-cleanup", dm.cleanupLoop); err != nil {
		dm.Stop()
		return fmt.Errorf("failed to start cleanup thread: %w", err)
	}

	dm.Debugf("Discovery started successfully on port %d", dm.broadcastPort)
	return nil
}

// Stop stops the discovery process
func (dm *DiscoveryManager) Stop() {
	dm.Debugf("Stopping discovery...")

	unregisterLocalDiscovery(dm)
	dm.mu.Lock()
	listenConn, broadcastConn := dm.listenConn, dm.broadcastConn
	dm.listenConn, dm.broadcastConn = nil, nil
	dm.mu.Unlock()
	if listenConn != nil {
		listenConn.Close()
	}
	if broadcastConn != nil {
		broadcastConn.Close()
	}

	dm.Debugf("Discovery stopped")
}

// GetPeers returns a list of currently known peers
func (dm *DiscoveryManager) GetPeers() []*types.PeerInfo {
	var peers []*types.PeerInfo
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != types.NodeID(dm.nodeID) { // Exclude ourselves
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

func (dm *DiscoveryManager) GetPeerMap() *syncmap.SyncMap[string, *types.PeerInfo] {
	return dm.peers
}

// GetPeerCount returns the number of active peers (excluding ourselves)
func (dm *DiscoveryManager) GetPeerCount() int {
	count := 0
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != types.NodeID(dm.nodeID) {
			count++
		}
		return true
	})
	return count
}

// broadcastLoop periodically broadcasts our presence
func (dm *DiscoveryManager) broadcastLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastUpdate time.Time
	// Send initial broadcast immediately
	dm.broadcast()

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Broadcast loop stopping")
			return
		case <-ticker.C:
			if time.Since(lastUpdate) > dm.getBroadcastInterval() {
				dm.broadcast()
				lastUpdate = time.Now()
			}
		}

	}
}

// broadcast sends our node information via UDP broadcast
func (dm *DiscoveryManager) broadcast() {
	dm.mu.RLock()
	connection, httpPort := dm.broadcastConn, dm.httpPort
	dm.mu.RUnlock()
	if connection == nil {
		dm.Debugf("Broadcast skipped: connection not available")
		return
	}

	message := fmt.Sprintf("CLUSTER_NODE:%s:%d", dm.nodeID, httpPort)
	if dm.mode == "local" {
		for _, address := range localDiscoveryAddresses(dm.broadcastPort) {
			if _, err := connection.WriteToUDP([]byte(message), address); err != nil {
				dm.logLocalAnnouncementFailure(address, err)
			}
		}
		return
	}

	_, err := connection.Write([]byte(message))
	if err != nil {
		dm.Debugf("Broadcast failed: %v", err)
	} else {
		//dm.Debugf("Broadcast sent: %s", message)
	}
}

// listenLoop listens for broadcasts from other nodes
func (dm *DiscoveryManager) listenLoop(ctx context.Context) {
	buffer := make([]byte, 1024)
	dm.mu.RLock()
	connection := dm.listenConn
	dm.mu.RUnlock()

	dm.Debugf("Starting discovery listen loop")

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Listen loop stopping")
			return
		default:
			if connection == nil {
				dm.Debugf("Listen skipped: connection not available")
				return
			}

			// Set read timeout to make this cancellable
			connection.SetReadDeadline(time.Now().Add(2 * time.Second))

			n, addr, err := connection.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Timeout is expected for cancellation
				}
				if strings.Contains(err.Error(), "closed network connection") {
					return
				}
				dm.Debugf("Listen error: %v", err)
				panic(err)
			}

			dm.handleDiscoveryMessage(string(buffer[:n]), addr)
		}
	}
}

// handleDiscoveryMessage processes a discovery message from another node
func (dm *DiscoveryManager) handleDiscoveryMessage(message string, addr *net.UDPAddr) {
	if !strings.HasPrefix(message, "CLUSTER_NODE:") {
		return // Not our protocol
	}

	// Parse message: CLUSTER_NODE:nodeID:httpPort
	parts := strings.Split(message, ":")
	if len(parts) != 3 {
		dm.Debugf("Invalid discovery message format: %s", message)
		return
	}

	nodeID := parts[1]
	httpPortStr := parts[2]

	httpPort, err := strconv.Atoi(httpPortStr)
	if err != nil || httpPort < 1 || httpPort > 65535 {
		dm.Debugf("Invalid port in discovery message: %s", httpPortStr)
		return
	}

	// Skip our own broadcasts
	if nodeID == "" || nodeID == dm.nodeID {
		return
	}

	// Update peer information
	peer := &types.PeerInfo{
		NodeID:   types.NodeID(nodeID),
		HTTPPort: httpPort,
		Address:  addr.IP.String(),
		LastSeen: time.Now(),
	}

	//fmt.Printf("Storing peer %+v at %s:%d at %v\n", peer, addr.IP.String(), httpPort, peer.LastSeen)
	_, isKnown := dm.peers.Load(nodeID)
	dm.peers.Store(nodeID, peer)

	if !isKnown {
		dm.logger.Printf("Discovered new peer: %s at %s:%d (seen by %s)", nodeID, peer.Address, httpPort, dm.nodeID)
	}
}

// cleanupLoop removes stale peers
func (dm *DiscoveryManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastUpdate time.Time

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Cleanup loop stopping")
			return
		case <-ticker.C:
			if time.Since(lastUpdate) > dm.getPeerTimeout() {
				dm.cleanupStalePeers()
				lastUpdate = time.Now()
			}

		}
	}
}

// cleanupStalePeers removes peers that haven't been seen recently
func (dm *DiscoveryManager) cleanupStalePeers() {
	now := time.Now()
	timeout := dm.getPeerTimeout()

	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if now.Sub(peer.LastSeen) > timeout && nodeID != dm.nodeID {
			dm.logger.Printf("Peer %s timed out, last seen at %v, %v seconds ago(timeout %v)", nodeID, peer.LastSeen, now.Sub(peer.LastSeen).Seconds(), timeout.Seconds())
			dm.peers.Delete(nodeID)
		}
		return true
	})
}

// SetBroadcastInterval configures how often to broadcast our presence
func (dm *DiscoveryManager) SetBroadcastInterval(interval time.Duration) {
	dm.mu.Lock()
	dm.broadcastInterval = interval
	dm.mu.Unlock()
	dm.Debugf("Broadcast interval set to %v", interval)
}

// SetPeerTimeout configures how long to wait before considering a peer dead
func (dm *DiscoveryManager) SetPeerTimeout(timeout time.Duration) {
	dm.mu.Lock()
	dm.peerTimeout = timeout
	dm.mu.Unlock()
	dm.Debugf("Peer timeout set to %v", timeout)
}

// GetDiscoveryStatus returns current discovery statistics
func (dm *DiscoveryManager) GetDiscoveryStatus() map[string]interface{} {
	var peerList []map[string]interface{}
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != types.NodeID(dm.nodeID) {
			peerList = append(peerList, map[string]interface{}{
				"node_id":   peer.NodeID,
				"address":   peer.Address,
				"http_port": peer.HTTPPort,
				"last_seen": peer.LastSeen.Unix(),
			})
		}
		return true
	})

	return map[string]interface{}{
		"node_id":               dm.nodeID,
		"broadcast_port":        dm.broadcastPort,
		"peer_count":            len(peerList),
		"peers":                 peerList,
		"broadcast_interval_ms": dm.getBroadcastInterval().Milliseconds(),
		"peer_timeout_ms":       dm.getPeerTimeout().Milliseconds(),
	}
}

// GetLocalAddress returns this node's IP address
func (dm *DiscoveryManager) GetLocalAddress() string {
	if dm.mode == "local" {
		return "127.0.0.1"
	}
	// Try to get address from our peers map (we store ourselves there)
	if peer, ok := dm.peers.Load(dm.nodeID); ok {
		return peer.Address
	}

	// Fallback: detect local IP by checking network interfaces
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		dm.logger.Printf("Failed to get network interfaces: %v", err)
		return "127.0.0.1"
	}

	// Find first non-loopback IPv4 address
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}

	// Last resort: return loopback
	return "127.0.0.1"
}

func (dm *DiscoveryManager) getBroadcastInterval() time.Duration {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.broadcastInterval
}

func (dm *DiscoveryManager) getPeerTimeout() time.Duration {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return dm.peerTimeout
}
