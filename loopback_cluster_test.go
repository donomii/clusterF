package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/donomii/clusterF/types"
)

func newLoopbackCluster(t *testing.T, id string, noStore bool) (*Cluster, *httptest.Server) {
	t.Helper()
	cluster := NewCluster(ClusterOpts{
		ID: id, DataDir: t.TempDir(), NoStore: noStore, StorageMajor: "rawfile",
		Logger: log.New(os.Stderr, id+" ", log.LstdFlags),
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/api/files/", corsMiddleware(false, cluster.Logger(), cluster, cluster.handleFilesAPI))
	mux.HandleFunc("/internal/files/", corsMiddleware(false, cluster.Logger(), cluster, cluster.handleInternalFilesAPI))
	mux.HandleFunc("/api/metadata/", cluster.handleMetadataAPI)
	mux.HandleFunc("/internal/metadata/", cluster.handleInternalMetadataAPI)
	mux.HandleFunc("/internal/search", cluster.handleInternalSearchAPI)
	mux.HandleFunc("/frogpond/update", cluster.handleFrogpondUpdate)
	mux.HandleFunc("/frogpond/fullstore", cluster.handleFrogpondFullStore)
	server := httptest.NewServer(mux)
	_, portText, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatal(err)
	}
	cluster.HTTPDataPort = port
	cluster.HttpDataClient.Timeout = 3 * time.Second
	cluster.httpClient.Timeout = 3 * time.Second
	registerLoopbackPeer(t, cluster, cluster, 99)
	t.Cleanup(func() {
		cluster.cancel()
		server.Close()
		cluster.partitionManager.FileStore().Close()
	})
	return cluster, server
}

func registerLoopbackPeer(t *testing.T, observer, peer *Cluster, diskFree int64) {
	t.Helper()
	info := &types.PeerInfo{
		NodeID: peer.ID(), Address: "127.0.0.1", HTTPPort: peer.HTTPDataPort, LastSeen: time.Now(),
	}
	observer.DiscoveryManager().GetPeerMap().Store(string(peer.ID()), info)
	data := types.NodeData{
		NodeID: peer.ID(), Address: info.Address, HTTPPort: info.HTTPPort,
		IsStorage: !peer.NoStore(), Available: true, DiskSize: 100, DiskFree: diskFree,
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	observer.frogpond.SetDataPoint("nodes/"+string(peer.ID()), encoded)
}

func setLoopbackHolders(cluster *Cluster, filePath string, holders ...types.NodeID) {
	partition := types.PartitionID(cluster.PartitionManager().CalculatePartitionName(filePath))
	cluster.partitionHolders.Store(partition, holders)
}
