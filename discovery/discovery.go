// discovery.go - Auto-discovery and peer networking
package discovery

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/donomii/clusterF/syncmap"
	"github.com/donomii/clusterF/threadmanager"
	"github.com/donomii/clusterF/types"
)

// UpdateNodeInfo updates our node's information (e.g., when HTTP port changes)
func (dm *DiscoveryManager) UpdateNodeInfo(nodeID string, httpPort int) {
	if nodeID == dm.nodeID {
		// Update our own HTTP port
		oldPort := dm.httpPort
		dm.httpPort = httpPort
		if oldPort != httpPort {
			dm.logger.Printf("HTTP port updated from %d to %d, broadcasting new info", oldPort, httpPort)
			// Send immediate broadcast with new port info
			dm.broadcast()
		}
	}
}

// DiscoveryManager handles auto-discovery of cluster peers
type DiscoveryManager struct {
	nodeID        string
	httpPort      int
	broadcastPort int
	logger        *log.Logger
	debug         bool

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
	return &DiscoveryManager{
		nodeID:            nodeID,
		httpPort:          httpPort,
		broadcastPort:     discoveryPort,
		logger:            logger,
		peers:             syncmap.NewSyncMap[string, *types.PeerInfo](),
		broadcastInterval: 5 * time.Second,
		peerTimeout:       15 * time.Second,
		threadManager:     tm,
		debug:             false,
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
	dm.broadcastInterval = broadcastInterval
	dm.peerTimeout = peerTimeout
}

// Start begins the discovery process
func (dm *DiscoveryManager) Start() error {
	//FIXME:  Put this in a loop so we handle network changes
	dm.Debugf("Starting discovery on port %d", dm.broadcastPort)

	// Create UDP listener with appropriate socket options (OS-specific)
	lc := newListenConfig()

	pc, err := lc.ListenPacket(context.Background(), "udp", fmt.Sprintf(":%d", dm.broadcastPort))
	if err != nil {
		return fmt.Errorf("failed to create listen socket on port %d: %w", dm.broadcastPort, err)
	}

	// Cast to UDPConn
	var ok bool
	dm.listenConn, ok = pc.(*net.UDPConn)
	if !ok {
		pc.Close()
		return fmt.Errorf("failed to cast to UDPConn")
	}

	dm.Debugf("UDP listen socket created successfully on %s", dm.listenConn.LocalAddr())

	// Set up UDP broadcast socket
	broadcastAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("255.255.255.255:%d", dm.broadcastPort))
	if err != nil {
		dm.listenConn.Close()
		log.Printf("failed to resolve broadcast address: %v.  Discovery disabled\n", err)
		return nil
	}

	dm.broadcastConn, err = net.DialUDP("udp", nil, broadcastAddr)
	if err != nil {
		dm.listenConn.Close()
		log.Printf("failed to create broadcast socket: %v", err)
		return nil
	}

	dm.Debugf("UDP broadcast socket created successfully to %s", dm.broadcastConn.RemoteAddr())

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

	if dm.listenConn != nil {
		dm.listenConn.Close()
		dm.listenConn = nil
	}
	if dm.broadcastConn != nil {
		dm.broadcastConn.Close()
		dm.broadcastConn = nil
	}

	dm.Debugf("Discovery stopped")
}

// GetPeers returns a list of currently known peers
func (dm *DiscoveryManager) GetPeers() []*types.PeerInfo {
	var peers []*types.PeerInfo
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != dm.nodeID { // Exclude ourselves
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

// GetPeerCount returns the number of active peers (excluding ourselves)
func (dm *DiscoveryManager) GetPeerCount() int {
	count := 0
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != dm.nodeID {
			count++
		}
		return true
	})
	return count
}

// broadcastLoop periodically broadcasts our presence
func (dm *DiscoveryManager) broadcastLoop(ctx context.Context) {
	ticker := time.NewTicker(dm.broadcastInterval)
	defer ticker.Stop()

	// Send initial broadcast immediately
	dm.broadcast()

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Broadcast loop stopping")
			return
		case <-ticker.C:
			dm.broadcast()
		}
		ticker.Reset(dm.broadcastInterval)
	}
}

// broadcast sends our node information via UDP broadcast
func (dm *DiscoveryManager) broadcast() {
	if dm.broadcastConn == nil {
		dm.Debugf("Broadcast skipped: connection not available")
		return
	}

	message := fmt.Sprintf("CLUSTER_NODE:%s:%d", dm.nodeID, dm.httpPort)

	_, err := dm.broadcastConn.Write([]byte(message))
	if err != nil {
		dm.Debugf("Broadcast failed: %v", err)
	} else {
		//dm.Debugf("Broadcast sent: %s", message)
	}
}

// listenLoop listens for broadcasts from other nodes
func (dm *DiscoveryManager) listenLoop(ctx context.Context) {
	buffer := make([]byte, 1024)

	dm.Debugf("Starting discovery listen loop")

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Listen loop stopping")
			return
		default:
			if dm.listenConn == nil {
				dm.Debugf("Listen skipped: connection not available")
				return
			}

			// Set read timeout to make this cancellable
			dm.listenConn.SetReadDeadline(time.Now().Add(1 * time.Second))

			n, addr, err := dm.listenConn.ReadFromUDP(buffer)
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
	if err != nil {
		dm.Debugf("Invalid port in discovery message: %s", httpPortStr)
		return
	}

	// Skip our own broadcasts
	if nodeID == dm.nodeID {
		return
	}

	// Update peer information
	peer := &types.PeerInfo{
		NodeID:   nodeID,
		HTTPPort: httpPort,
		Address:  addr.IP.String(),
		LastSeen: time.Now(),
	}

	_, isNew := dm.peers.LoadOrStore(nodeID, peer)
	if !isNew {
		dm.peers.Store(nodeID, peer)
	}
	isNew = !isNew

	if isNew {
		dm.Debugf("Discovered new peer: %s at %s:%d (seen by %s)", nodeID, peer.Address, httpPort, dm.nodeID)
	}
}

// cleanupLoop removes stale peers
func (dm *DiscoveryManager) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			dm.Debugf("Cleanup loop stopping")
			return
		case <-ticker.C:
			dm.cleanupStalePeers()
		}
	}
}

// cleanupStalePeers removes peers that haven't been seen recently
func (dm *DiscoveryManager) cleanupStalePeers() {
	now := time.Now()

	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if now.Sub(peer.LastSeen) > dm.peerTimeout && nodeID != dm.nodeID {
			//dm.logger.Printf("Peer %s timed out, removing", nodeID)
			dm.peers.Delete(nodeID)
		}
		return true
	})
}

// SetBroadcastInterval configures how often to broadcast our presence
func (dm *DiscoveryManager) SetBroadcastInterval(interval time.Duration) {
	dm.broadcastInterval = interval
	dm.Debugf("Broadcast interval set to %v", interval)
}

// SetPeerTimeout configures how long to wait before considering a peer dead
func (dm *DiscoveryManager) SetPeerTimeout(timeout time.Duration) {
	dm.peerTimeout = timeout
	dm.Debugf("Peer timeout set to %v", timeout)
}

// GetDiscoveryStatus returns current discovery statistics
func (dm *DiscoveryManager) GetDiscoveryStatus() map[string]interface{} {
	var peerList []map[string]interface{}
	dm.peers.Range(func(nodeID string, peer *types.PeerInfo) bool {
		if peer.NodeID != dm.nodeID {
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
		"broadcast_interval_ms": dm.broadcastInterval.Milliseconds(),
		"peer_timeout_ms":       dm.peerTimeout.Milliseconds(),
	}
}
