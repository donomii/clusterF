package discovery

import (
	"net"
	"testing"
	"time"

	"github.com/donomii/clusterF/threadmanager"
)

func TestLocalDiscoveryPublishesChangedHTTPPort(t *testing.T) {
	t.Setenv("CLUSTER_DISCOVERY_MODE", "local")
	firstThreads := threadmanager.NewThreadManager("first", newTestLogger())
	secondThreads := threadmanager.NewThreadManager("second", newTestLogger())
	first := NewDiscoveryManager("first", 32001, 12000, firstThreads, newTestLogger())
	second := NewDiscoveryManager("second", 32002, 12000, secondThreads, newTestLogger())
	for _, manager := range []*DiscoveryManager{first, second} {
		manager.SetTimings(time.Second, 10*time.Second)
		if err := manager.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		first.Stop()
		second.Stop()
		firstThreads.Shutdown()
		secondThreads.Shutdown()
	})
	first.UpdateNodeInfo("first", 32003)
	WaitForConditionT(t, "updated peer HTTP port", func() bool {
		peer, exists := second.GetPeerMap().Load("first")
		return exists && peer.HTTPPort == 32003 && peer.Address == "127.0.0.1"
	}, 10, 3000)
	if !first.listenConn.LocalAddr().(*net.UDPAddr).IP.IsLoopback() {
		t.Fatalf("local discovery listener is not loopback: %s", first.listenConn.LocalAddr())
	}
}
