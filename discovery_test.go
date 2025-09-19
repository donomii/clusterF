package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"testing"
	"time"

	"github.com/donomii/clusterF/threadmanager"
)

// newTestLogger returns a quiet logger for tests
func newTestLogger() *log.Logger {
	return log.New(io.Discard, "", log.LstdFlags|log.Lshortfile)
}

// pickDiscoveryPort picks a pseudo-random UDP port for discovery tests
func pickDiscoveryPort() int {
	// Avoid common ports; keep within ephemeral range
	rand.Seed(time.Now().UnixNano())
	return 20000 + rand.Intn(20000)
}

// waitForPeerCount waits until the discovery manager has at least n peers
func waitForPeerCount(t *testing.T, dm *DiscoveryManager, n int, timeoutMs int) {
	t.Helper()
	WaitForConditionT(t, "peer discovery", func() bool {
		return dm.GetPeerCount() >= n
	}, 50, timeoutMs)
}

func TestDiscovery_SingleNode_NoSelfPeer(t *testing.T) {
	tm := threadmanager.NewThreadManager("node-1", newTestLogger())
	dm := NewDiscoveryManager("node-1", 31001, pickDiscoveryPort(), tm, newTestLogger())
	// Fast timings to accelerate test
	dm.SetTimings(100*time.Millisecond, 2*time.Second)

	if err := dm.Start(); err != nil {
		t.Fatalf("failed to start discovery manager: %v", err)
	}
	defer func() { dm.Stop(); tm.Shutdown() }()

	// Give it a moment to broadcast/listen; ensure no self is recorded
	time.Sleep(400 * time.Millisecond)

	if c := dm.GetPeerCount(); c != 0 {
		t.Fatalf("expected 0 peers, got %d", c)
	}
}

func TestDiscovery_TwoNodes_DiscoverEachOther(t *testing.T) {
	port := pickDiscoveryPort()

	tm1 := threadmanager.NewThreadManager("n1", newTestLogger())
	dm1 := NewDiscoveryManager("n1", 31101, port, tm1, newTestLogger())
	dm1.SetTimings(100*time.Millisecond, 2*time.Second)

	tm2 := threadmanager.NewThreadManager("n2", newTestLogger())
	dm2 := NewDiscoveryManager("n2", 31102, port, tm2, newTestLogger())
	dm2.SetTimings(100*time.Millisecond, 2*time.Second)

	if err := dm1.Start(); err != nil {
		t.Fatalf("dm1 start: %v", err)
	}
	if err := dm2.Start(); err != nil {
		t.Fatalf("dm2 start: %v", err)
	}
	defer func() {
		dm1.Stop()
		dm2.Stop()
		tm1.Shutdown()
		tm2.Shutdown()
	}()

	waitForPeerCount(t, dm1, 1, 5000)
	waitForPeerCount(t, dm2, 1, 5000)
}

func TestDiscovery_ThreeNodes_AllSeePeers(t *testing.T) {
	port := pickDiscoveryPort()

	tms := []*threadmanager.ThreadManager{
		threadmanager.NewThreadManager("n0", newTestLogger()),
		threadmanager.NewThreadManager("n1", newTestLogger()),
		threadmanager.NewThreadManager("n2", newTestLogger()),
	}
	dms := []*DiscoveryManager{
		NewDiscoveryManager("n0", 31200, port, tms[0], newTestLogger()),
		NewDiscoveryManager("n1", 31201, port, tms[1], newTestLogger()),
		NewDiscoveryManager("n2", 31202, port, tms[2], newTestLogger()),
	}
	for _, dm := range dms {
		dm.SetTimings(120*time.Millisecond, 3*time.Second)
		if err := dm.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	defer func() {
		for _, dm := range dms {
			dm.Stop()
		}
		for _, tm := range tms {
			tm.Shutdown()
		}
	}()

	// Each node should see at least 1 peer (not necessarily all, to tolerate broadcast variance)
	for _, dm := range dms {
		waitForPeerCount(t, dm, 1, 7000)
	}
}

func TestDiscovery_InvalidMessagesIgnored(t *testing.T) {
	tm := threadmanager.NewThreadManager("node-x", newTestLogger())
	dm := NewDiscoveryManager("node-x", 31301, pickDiscoveryPort(), tm, newTestLogger())

	// Feed invalid messages directly into handler without opening sockets
	dm.handleDiscoveryMessage("HELLO_WORLD", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	dm.handleDiscoveryMessage("CLUSTER_NODE:badformat", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	dm.handleDiscoveryMessage("CLUSTER_NODE:n1:notaport", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	dm.handleDiscoveryMessage("CLUSTER_NODE:node-x:31301", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}) // self

	if c := dm.GetPeerCount(); c != 0 {
		t.Fatalf("expected 0 peers after invalid/self messages, got %d", c)
	}
}

func TestDiscovery_HandleMessageAddsPeer(t *testing.T) {
	tm := threadmanager.NewThreadManager("node-a", newTestLogger())
	dm := NewDiscoveryManager("node-a", 31401, pickDiscoveryPort(), tm, newTestLogger())

	dm.handleDiscoveryMessage("CLUSTER_NODE:node-b:31402", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})

	peers := dm.GetPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].NodeID != "node-b" || peers[0].HTTPPort != 31402 {
		t.Fatalf("unexpected peer info: %+v", peers[0])
	}
}

func TestDiscovery_CleanupRemovesStalePeers(t *testing.T) {
	tm := threadmanager.NewThreadManager("node-a", newTestLogger())
	dm := NewDiscoveryManager("node-a", 31501, pickDiscoveryPort(), tm, newTestLogger())
	dm.SetTimings(100*time.Millisecond, 200*time.Millisecond)

	// Manually add a stale peer
	dm.peersMux.Lock()
	dm.peers["node-b"] = &PeerInfo{NodeID: "node-b", HTTPPort: 31502, Address: "127.0.0.1", LastSeen: time.Now().Add(-1 * time.Hour)}
	dm.peersMux.Unlock()

	// Run cleanup once
	dm.cleanupStalePeers()

	if c := dm.GetPeerCount(); c != 0 {
		t.Fatalf("expected 0 peers after cleanup, got %d", c)
	}
}

func TestDiscovery_StatusSnapshot(t *testing.T) {
	port := pickDiscoveryPort()
	tm1 := threadmanager.NewThreadManager("s1", newTestLogger())
	dm1 := NewDiscoveryManager("s1", 31601, port, tm1, newTestLogger())
	dm1.SetTimings(100*time.Millisecond, 2*time.Second)

	tm2 := threadmanager.NewThreadManager("s2", newTestLogger())
	dm2 := NewDiscoveryManager("s2", 31602, port, tm2, newTestLogger())
	dm2.SetTimings(100*time.Millisecond, 2*time.Second)

	if err := dm1.Start(); err != nil {
		t.Fatalf("dm1 start: %v", err)
	}
	if err := dm2.Start(); err != nil {
		t.Fatalf("dm2 start: %v", err)
	}
	defer func() { dm1.Stop(); dm2.Stop(); tm1.Shutdown(); tm2.Shutdown() }()

	waitForPeerCount(t, dm1, 1, 5000)

	st := dm1.GetDiscoveryStatus()
	if st["peer_count"].(int) < 1 {
		t.Fatalf("expected peer_count >= 1, got %v", st["peer_count"])
	}
	if st["broadcast_port"].(int) != port {
		t.Fatalf("expected broadcast_port %d, got %v", port, st["broadcast_port"])
	}
}

func TestDiscovery_Stop_ReleasesResources(t *testing.T) {
	tm := threadmanager.NewThreadManager("node-z", newTestLogger())
	dm := NewDiscoveryManager("node-z", 31701, pickDiscoveryPort(), tm, newTestLogger())
	dm.SetTimings(100*time.Millisecond, 2*time.Second)

	if err := dm.Start(); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if dm.listenConn == nil || dm.broadcastConn == nil {
		t.Fatal("expected non-nil connections after start")
	}

	// Stop discovery and then the thread manager
	dm.Stop()
	tm.Shutdown()

	if dm.listenConn != nil || dm.broadcastConn != nil {
		t.Fatal("expected nil connections after Stop")
	}
}

// Large-scale discovery test with ~100 nodes sharing a single discovery port.
// This exercises socket reuse, broadcast/listen loops, and basic liveness at scale.
// Skips under -short to avoid heavy runtime in CI.
func TestDiscovery_Scale_OneHundredNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 100-node discovery test in -short mode")
	}

	const n = 100
	port := pickDiscoveryPort()

	// Stagger HTTP ports just to fill the PeerInfo; not actually serving HTTP here.
	baseHTTP := 33000

	t.Logf("starting %d discovery managers on port %d", n, port)

	tms := make([]*threadmanager.ThreadManager, 0, n)
	dms := make([]*DiscoveryManager, 0, n)

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("scale-%03d", i)
		tm := threadmanager.NewThreadManager(id, newTestLogger())
		dm := NewDiscoveryManager(id, baseHTTP+i, port, tm, newTestLogger())
		// Faster broadcasts, moderate timeout to keep peers around
		dm.SetTimings(150*time.Millisecond, 8*time.Second)

		if err := dm.Start(); err != nil {
			t.Fatalf("start failed for %s: %v", id, err)
		}
		tms = append(tms, tm)
		dms = append(dms, dm)
		// Small stagger to avoid thundering herd on launch
		time.Sleep(10 * time.Millisecond)
	}

	defer func() {
		for _, dm := range dms {
			dm.Stop()
		}
		for _, tm := range tms {
			tm.Shutdown()
		}
	}()

	// Condition: at least 80% of nodes should see at least 1 peer within 12s.
	target := int(float64(n) * 0.80)
	WaitForConditionT(t, "80% nodes have >=1 peer", func() bool {
		seen := 0
		for _, dm := range dms {
			if dm.GetPeerCount() >= 1 {
				seen++
			}
		}
		return seen >= target
	}, 100, 12000)

	// Report simple stats to help debug flakiness while keeping assertions soft.
	minPeers, maxPeers, total := 1<<30, -1, 0
	for _, dm := range dms {
		c := dm.GetPeerCount()
		if c < minPeers {
			minPeers = c
		}
		if c > maxPeers {
			maxPeers = c
		}
		total += c
	}
	avg := float64(total) / float64(n)
	t.Logf("peer count stats across %d nodes => min=%d avg=%.1f max=%d", n, minPeers, avg, maxPeers)
}
